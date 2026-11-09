package com.tencent.angel.spark.ml.time_series.transformation


import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait BoxCoxParams extends TimeSeriesParams with DefaultParamsWritable {
  val lambda: IntParam = new IntParam(this, "lambda",
    "powers to use for the Box-Cox transformation")
  def getLambda: Double = $(lambda)
}


class BoxCox(override val uid: String) extends Transformer with BoxCoxParams{
  def this() = this(Identifiable.randomUID("BoxCox"))

  def setLambda(value: Int): this.type = set(lambda, value)

  setDefault(lambda -> 1)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val timeVector = data.map(x => x._1)
    val dataVector = Vectors.dense(data.map(x => x._2))

    val bcResult = dataVector.toArray
    val n = dataVector.size
    var i = 0

    while (i < n) {
      // elements prior to starting point are copied over without modification
      if (${lambda} == 0.0) { // 对数变换
        bcResult(i) = math.log(dataVector(i))
      } else {
        bcResult(i) = (math.pow(dataVector(i), $(lambda)) - 1) / $(lambda)  // Box-Cox 变换
      }
      i += 1
    }
    val result = timeVector.zip(bcResult)

    val spark = dataset.sparkSession
    import spark.implicits._

    val dataFrame = spark.createDataset(result).toDF($(timeCol), $(timeSeriesCol))

    dataFrame
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}
