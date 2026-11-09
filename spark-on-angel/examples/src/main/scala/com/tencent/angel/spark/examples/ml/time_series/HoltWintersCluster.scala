package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.tencent.angel.spark.ml.time_series.timeModel._
import com.tencent.angel.spark.ml.core.ArgsUtil

object HoltWintersCluster {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
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

    val maxEval = params.getOrElse("maxEval", "10000").toInt
    val maxIter = params.getOrElse("maxIter", "10000").toInt
    val actionType = params.getOrElse("actionType", "train")
    val modelPath = params.getOrElse("modelPath", null)
    val period = params.getOrElse("period", "12").toInt
    val modelType = params.getOrElse("modelType", "additive")

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")


    actionType match {
      case "train" =>
        val model = train(sc, input, timeCol, timeSeriesCol, maxEval, maxIter, period, modelType, partitionNum, sep, sampleRate)
        sc.parallelize(Seq(model), 1).saveAsObjectFile(modelPath)

      case "predict" =>
        val model = sc.objectFile[HoltWintersModel](modelPath).first()
        predict(sc, input, output, output1, timeCol, timeSeriesCol, period, modelType, model, partitionNum, sep, sampleRate)

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
            period: Int,
            modelType: String,
            partitionNum: Int,
            sep: String,
            sampleRate: Double): HoltWintersModel = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)
    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val model = new HoltWinters()
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setMaxEval(maxEval)
      .setMaxIter(maxIter)
      .setPeriod(period)
      .setModelType(modelType)
      .fit(data)

    model
  }

  def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("time", DoubleType),
      StructField("level", DoubleType),
      StructField("trend", DoubleType),
      StructField("season", DoubleType)))
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              output1: String,
              timeCol: Int,
              timeSeriesCol: Int,
              period: Int,
              modelType: String,
              model: HoltWintersModel,
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
      .setPeriod(period)
      .setModelType(modelType)
      .transform(data)

    // save results for level, trend and season
    val data2 = data.select(timeColName, timeSeriesColName).rdd.map {
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data2.map(x => x._2.toDouble))
    val result = model.getHoltWintersComponents(dataVector)

    val length = dataVector.size - period
    val resCombine = data2.map(x => x._1).zip(result._2.toArray.take(length)).
      zip(result._3.toArray.take(length)).zip(result._4.toArray.take(length))

    val resRDD = data.sparkSession.sparkContext.parallelize(
      resCombine.map(x => Row(x._1._1._1, x._1._1._2, x._1._2, x._2)))

    val structType = transformSchema(data.schema)
    val resultDF = data.sparkSession.createDataFrame(resRDD, structType)

    DataSaver.save(resultDF, output, sep)
    DataSaver.save(outputDF, output1, sep)
  }
}
