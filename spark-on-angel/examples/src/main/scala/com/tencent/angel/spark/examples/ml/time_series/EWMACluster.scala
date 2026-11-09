package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.tencent.angel.spark.ml.time_series.timeModel._

object EWMACluster {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val output = params.getOrElse("output", null)
    val timeCol = params.getOrElse("timeCol", "0").toInt
    val timeSeriesCol = params.getOrElse("timeSeriesCol", "1").toInt
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val lambda = params.getOrElse("lambda", "0.2").toDouble

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
