package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.tencent.angel.spark.ml.time_series.timeModel._

/**
 * Created by isakjiang on 2021/02/26.
 */
object AutoARIMACluster {
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

    val nFuture = params.getOrElse("nFuture", "12").toInt
    val maxEval = params.getOrElse("maxEval", "0").toInt
    val maxIter = params.getOrElse("maxIter", "0").toInt
    val maxP = params.getOrElse("maxP", "1").toInt
    val maxD = params.getOrElse("maxD", "0").toInt
    val maxQ = params.getOrElse("maxQ", "1").toInt
    val includeIntercept = params.getOrElse("intercept", "true").toBoolean

    val actionType = params.getOrElse("actionType", "train")
    val modelPath = params.getOrElse("modelPath", null)


    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    actionType match {
      case "train" =>
        val model = train(sc, input, output, timeCol, timeSeriesCol, maxEval, maxIter,
          maxP, maxD, maxQ, includeIntercept, partitionNum, sep, sampleRate)
        sc.parallelize(Seq(model), 1).saveAsObjectFile(modelPath)

      case "predict" =>
        val model = sc.objectFile[ARIMAModel](modelPath).first()
        predict(sc, input, output, nFuture, timeCol, timeSeriesCol, model, partitionNum, sep, sampleRate)

      case _ =>
        LOG.error(s"$actionType do not support")
    }
  }

  def train(sc: SparkContext,
            input: String,
            output: String,
            timeCol: Int,
            timeSeriesCol: Int,
            maxEval: Int,
            maxIter: Int,
            maxP: Int,
            maxD: Int,
            maxQ: Int,
            includeIntercept: Boolean,
            partitionNum: Int,
            sep: String,
            sampleRate: Double): ARIMAModel = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val model = new AutoARIMA()
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setMaxEval(maxEval)
      .setMaxIter(maxIter)
      .setMaxP(maxP)
      .setMaxD(maxD)
      .setMaxQ(maxQ)
      .setIncludeIntercept(includeIntercept)
      .fit(data)

    val outputDF = model
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setNFuture(0)
      .transform(data)

    val returnDF = outputDF.withColumn("residual", outputDF(timeSeriesColName) - outputDF("prediction"))

    DataSaver.save(returnDF, output, sep)
    model
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              nFuture: Int,
              timeCol: Int,
              timeSeriesCol: Int,
              model: ARIMAModel,
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
      .setNFuture(nFuture)
      .transform(data)

    DataSaver.save(outputDF, output, sep)
  }

}
