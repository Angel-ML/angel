package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object GARCHLocal {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val input = "data/mlTestData/example_air_passengers_unix_data"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outACF"

    val timeCol = 0
    val timeSeriesCol = 1

    val maxEval = 100
    val maxIter = 100

    val actionType = "predict"

    val modelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/modelARIMA"

    val partitionNum = 1
    val sep = ","
    val sampleRate = 1

    val mode = "local"

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