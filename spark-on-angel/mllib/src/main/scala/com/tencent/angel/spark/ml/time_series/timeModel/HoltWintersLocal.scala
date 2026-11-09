package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object HoltWintersLocal {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val input = "data/mlTestData/example_air_passengers_unix_data"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outACF"
    val output1 = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outACF1"

    val timeCol = 0
    val timeSeriesCol = 1

    val maxEval = 100
    val maxIter = 100
    val period = 12
    val modelType = "additive"

    val actionType = "predict"
    val modelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/model"

    val partitionNum = 1
    val sep = ","
    val sampleRate = 1

    val mode = "local"

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
