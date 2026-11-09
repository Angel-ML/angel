package com.tencent.angel.spark.ml.time_series.test

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.tencent.angel.spark.ml.util.UnivariateTimeSeriesUtil

trait LBTestParams extends TimeSeriesParams with DefaultParamsWritable {
  val maxLag: IntParam = new IntParam(this, "maxLag",
    "Degrees of freedom of Chi-Squared distribution")

  def setMaxLag(value: Int): this.type = set(maxLag, value)

}

class LBTest(override val uid: String) extends Transformer with LBTestParams {

  setDefault(maxLag -> 1)

  def this() = this(Identifiable.randomUID("LBTest"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))

    val result = LBTest.lbtest(dataVector, $(maxLag))

    val spark = dataset.sparkSession
    import spark.implicits._
    val reDF = spark.createDataset(Array(result)).toDF("lbStat", "pValue")
    reDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("lbStat", DoubleType),
      StructField("pValue", DoubleType)))
  }
}

object LBTest {

  /**
   * Ljung-Box test for serial correlation in residuals up to lag `maxLag`. The null hypothesis
   * is that values are independently distributed up to the given lag. The alternate hypothesis
   * is that serial correlation is present. The test statistic follows a Chi-Squared distribution
   * with `maxLag` degrees of freedom. See [[https://en.wikipedia.org/wiki/Ljung%E2%80%93Box_test]]
   * for more information.
   *
   * @return the test statistic and the p-value associated with it.
   */
  def lbtest(residuals: Vector, maxLag: Int): (Double, Double) = {
    val autoCorrs = UnivariateTimeSeriesUtil.autocorr(residuals, maxLag)
    val n = residuals.size
    val adjAutoCorrs = autoCorrs.toArray.zipWithIndex.map { case (p, k) =>
      (p * p) / (n - k - 1)
    }
    val testStatistic = n * (n + 2) * adjAutoCorrs.sum
    val pValue = 1 - new ChiSquaredDistribution(maxLag).cumulativeProbability(testStatistic)
    (testStatistic, pValue)
  }
}
