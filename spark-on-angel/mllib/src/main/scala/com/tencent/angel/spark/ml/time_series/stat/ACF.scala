package com.tencent.angel.spark.ml.time_series.stat

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import  com.tencent.angel.spark.ml.util.UnivariateTimeSeriesUtil

trait ACFParams extends TimeSeriesParams {
  final val maxLag = new Param[Int](this, "maxLag", "max lag")

  def setMaxLag(value: Int): this.type = set(maxLag, value)

}

class ACF(override val uid: String) extends Transformer with ACFParams {
  def this() = this(Identifiable.randomUID("ACF"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val ts = Vectors.dense(data.map(x => x._2))

    // return ACF values (first half) and corresponding confidence intervals (second half)
    val autoCorrs = UnivariateTimeSeriesUtil.autocorr(ts, $(maxLag)).toArray

    val corr = new Array[Double]($(maxLag))
    // calculate ACF confidence interval
    // Ref: https://stats.stackexchange.com/questions/129052/acf-and-pacf-formula
    // Ref: https://stats.stackexchange.com/questions/58090/confidence-bands-in-case-of-fitting-arima-in-r
    val cumSquareSum = new Array[Double]($(maxLag))
    cumSquareSum(0) = autoCorrs(0) * autoCorrs(0)
    corr(0) = 1.96 * math.sqrt(1.0 / ts.size)
    var p = 2
    while (p < $(maxLag) + 1) {
      cumSquareSum(p - 1) = cumSquareSum(p - 2) + autoCorrs(p - 1) * autoCorrs(p - 1)
      corr(p - 1) = 1.96 * math.sqrt(1.0 / ts.size * (1 + 2 * cumSquareSum(p - 2)))
      p += 1
    }

    val resultDF = autoCorrs.zip(corr).zipWithIndex

    val resRDD = dataset.sparkSession.sparkContext.parallelize(resultDF.map(x => Row(x._1._1, x._1._2, x._2.toDouble)))

    val structType = transformSchema(dataset.schema)

    dataset.sparkSession.createDataFrame(resRDD, structType)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("ACF", DoubleType),
      StructField("CI", DoubleType),
      StructField("index", DoubleType)))
  }
}
