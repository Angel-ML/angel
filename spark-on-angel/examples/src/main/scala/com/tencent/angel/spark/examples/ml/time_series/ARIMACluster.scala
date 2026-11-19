package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.tencent.angel.spark.ml.time_series.timeModel._

object ARIMACluster {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val output = params.getOrElse("output", null)
    val output1 = params.getOrElse("output1", null)

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
    val p = params.getOrElse("p", "1").toInt
    val d = params.getOrElse("d", "0").toInt
    val q = params.getOrElse("q", "1").toInt
    val includeIntercept = params.getOrElse("includeIntercept", "true").toBoolean
    val method = params.getOrElse("method", "css-cgd")

    val actionType = params.getOrElse("actionType", "train")
    val modelPath = params.getOrElse("modelPath", null)

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    actionType match {
      case "train" =>
        val model = train(sc, input, output, output1, timeCol, timeSeriesCol, maxEval, maxIter,
          p, d, q, includeIntercept, method, partitionNum, sep, sampleRate)

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
            output1: String,
            timeCol: Int,
            timeSeriesCol: Int,
            maxEval: Int,
            maxIter: Int,
            p: Int,
            d: Int,
            q: Int,
            includeIntercept: Boolean,
            method: String,
            partitionNum: Int,
            sep: String,
            sampleRate: Double): ARIMAModel = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val model = new ARIMA()
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setMaxEval(maxEval)
      .setMaxIter(maxIter)
      .setP(p)
      .setD(d)
      .setQ(q)
      .setIncludeIntercept(includeIntercept)
      .setMethod(method)
      .fit(data)

    // calculate and save AIC & BIC
    val tsdata = data.select(timeColName, timeSeriesColName).rdd.map {
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()
    val tsVector = Vectors.dense(tsdata.map(x => x._2.toDouble))

    val aicResult = model.approxAIC(tsVector)
    val bicResult = model.approxBIC(tsVector)

    val criData = Seq(
      (aicResult, "AIC"),
      (bicResult, "BIC")
    )
    val spark = data.sparkSession
    import spark.implicits._
    val criDF = spark.createDataset(criData).toDF("Value", "Criterion")

    val outputDF = model
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setNFuture(0)
      .transform(data)

    val returnDF = outputDF.withColumn("residual", outputDF(timeSeriesColName) - outputDF("prediction"))

    DataSaver.save(criDF, output1, sep)
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
