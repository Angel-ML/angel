package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.tencent.angel.spark.ml.time_series.timeModel._
import com.tencent.angel.spark.ml.core.ArgsUtil

object GARCHCluster {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
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

    val maxEval = params.getOrElse("maxEval", "10000").toInt
    val maxIter = params.getOrElse("maxIter", "10000").toInt
    val actionType = params.getOrElse("actionType", "train")
    val modelPath = params.getOrElse("modelPath", null)

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    actionType match {
      case "train" =>
        val model = train(sc, input, timeCol, timeSeriesCol, maxEval, maxIter, partitionNum, sep, sampleRate)
        sc.parallelize(Seq(model), 1).saveAsObjectFile(modelPath)

      case "predict" =>
        val model = sc.objectFile[GARCHModel](modelPath).first()
        predict(sc, input, output, timeCol, timeSeriesCol, model, partitionNum, sep, sampleRate)

      case _ =>
        LOG.error(s"$actionType do not support")
    }
  }

  def train(sc: SparkContext,
            input: String,
            timeCol: Int,
            timeSeriesCol: Int,
            maxEval: Int,
            maxIter: Int,
            partitionNum: Int,
            sep: String,
            sampleRate: Double): GARCHModel = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val model = new GARCH()
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setMaxEval(maxEval)
      .setMaxIter(maxIter)
      .fit(data)
    model
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              timeCol: Int,
              timeSeriesCol: Int,
              model: GARCHModel,
              partitionNum: Int,
              sep: String,
              sampleRate: Double): Unit = {
    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val outputDF = model
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .transform(data)

    DataSaver.save(outputDF, output, sep)
  }
}
