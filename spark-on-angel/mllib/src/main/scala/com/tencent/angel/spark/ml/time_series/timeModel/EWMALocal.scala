package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object EWMALocal {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val input = "data/mlTestData/example_air_passengers_unix_data"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outACF"

    val timeCol = 0
    val timeSeriesCol = 1

    val lambda = 0.2

    val partitionNum = 1
    val sep = ","
    val sampleRate = 1

    val mode = "local"

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    predict(sc, input, output, timeCol, timeSeriesCol, lambda, partitionNum, sep, sampleRate)
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              timeCol: Int,
              timeSeriesCol: Int,
              lambda: Double,
              partitionNum: Int,
              sep: String,
              sampleRate: Double): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val model = new EWMAModel(lambda)
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)

    val outputDF = model
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .transform(data)
    DataSaver.save(outputDF, output, sep)

  }

}
