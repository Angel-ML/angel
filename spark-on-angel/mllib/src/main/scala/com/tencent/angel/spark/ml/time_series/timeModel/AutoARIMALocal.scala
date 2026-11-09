package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.slf4j.LoggerFactory

/**
 * Created by isakjiang on 2021/02/26.
 */
object AutoARIMALocal {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val input = "data/mlTestData/example_air_passengers_unix_data_with_index"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outACF"

    val timeCol = 0
    val timeSeriesCol = 1
    val indexCol = 2
    val nFuture = 6
    val maxEval = 10
    val maxIter = 10
    val maxP = 2
    val maxD = 2
    val maxQ = 2
    val includeIntercept = true

    val actionType = "predict"
    val modelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/model"
    val modelParametersPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/modelParameters"
    val partitionNum = 1
    val sep = ","
    val sampleRate = 1

    val mode = "local"

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    actionType match {
      case "train" =>
        train(sc, input, output, timeCol, timeSeriesCol, indexCol, maxEval, maxIter,
          maxP, maxD, maxQ, includeIntercept, partitionNum, sep, sampleRate, modelPath, modelParametersPath)

      case "predict" =>
        predict(sc, input, output, modelPath, nFuture, timeCol, timeSeriesCol, indexCol, partitionNum, sep, sampleRate)

      case _ =>
        LOG.error(s"$actionType do not support")
    }
  }

  def train(sc: SparkContext,
            input: String,
            output: String,
            timeCol: Int,
            timeSeriesCol: Int,
            indexCol: Int,
            maxEval: Int,
            maxIter: Int,
            maxP: Int,
            maxD: Int,
            maxQ: Int,
            includeIntercept: Boolean,
            partitionNum: Int,
            sep: String,
            sampleRate: Double,
            modelPath: String,
            modelParametersPath: String): Unit = {
    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)
    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name
    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    if (indexCol != -1 && indexCol != null) {
      val indexColName = data.schema.fields(indexCol).name
      val indexData = data.select(indexColName).rdd.distinct()
        .map { case Row(index: String) => index }.filter(a => a != null).collect()

      val outputArray = new Array[sql.DataFrame](indexData.size)
      val parametersArray = new Array[sql.DataFrame](indexData.size)
      var indexForOutput = 0

      for (index_i <- indexData) {
        val dataTmp = data.where(s"$indexColName == $index_i")
        val (returnDF, parametersDF, model) = trainOnce(sc, dataTmp, timeColName, timeSeriesColName, maxEval, maxIter, maxP, maxD, maxQ, includeIntercept)
        outputArray(indexForOutput) = returnDF.withColumn("index", lit(index_i))
        parametersArray(indexForOutput) = parametersDF.withColumn("index", lit(index_i))
        indexForOutput += 1
        sc.parallelize(Seq(model), 1).saveAsObjectFile(modelPath + s"/$index_i")
      }

      val outputUnion = outputArray.toSeq.reduce(_ union _).repartition(partitionNum)
      DataSaver.save(outputUnion, output, sep)

      if (modelParametersPath != null) {
        val parametersUnion = parametersArray.toSeq.reduce(_ union _).repartition(partitionNum)
        DataSaver.save(parametersUnion, modelParametersPath, sep)
      }
    }
    else {
      val (returnDF, parametersDF, model) = trainOnce(sc, data, timeColName, timeSeriesColName, maxEval, maxIter, maxP, maxD, maxQ, includeIntercept)
      sc.parallelize(Seq(model), 1).saveAsObjectFile(modelPath)
      DataSaver.save(returnDF, output, sep)
      if (modelParametersPath != null) {
        DataSaver.save(parametersDF, modelParametersPath, sep)
      }
    }
  }

  def trainOnce(sc: SparkContext, data: sql.DataFrame,
                timeColName: String,
                timeSeriesColName: String,
                maxEval: Int,
                maxIter: Int,
                maxP: Int,
                maxD: Int,
                maxQ: Int,
                includeIntercept: Boolean): (sql.DataFrame, sql.DataFrame, ARIMAModel) = {

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


    val coff = model.coefficients

    val parameters = if (model.hasIntercept) {
      val interceptTerm = coff(0).toString
      val ARParameters = coff.slice(1, 1 + model.p).mkString("|")
      val MAParameters = coff.slice(1 + model.p, 1 + model.p + model.q).mkString("|")
      val dParameters = model.d.toString
      Seq((interceptTerm, ARParameters, MAParameters, dParameters))
    }
    else {
      val interceptTerm = 0.toString
      val ARParameters = coff.slice(0, model.p).mkString("|")
      val MAParameters = coff.slice(model.p, model.p + model.q).mkString("|")
      val dParameters = model.d.toString
      Seq((interceptTerm, ARParameters, MAParameters, dParameters))
    }

    import data.sparkSession.sqlContext.implicits._
    val parametersDF = sc.parallelize(parameters).toDF("interceptTerm", "ARParameters", "MAParameters", "dParameters")

    val outputDF = model
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setNFuture(0)
      .transform(data)

    val returnDF = outputDF.withColumn("residual", outputDF(timeSeriesColName) - outputDF("prediction"))
    (returnDF, parametersDF, model)
  }


  def predict(sc: SparkContext,
              input: String,
              output: String,
              modelPath: String,
              nFuture: Int,
              timeCol: Int,
              timeSeriesCol: Int,
              indexCol: Int,
              partitionNum: Int,
              sep: String,
              sampleRate: Double): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)
    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name
    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    if (indexCol != -1 && indexCol != null) {
      val indexColName = data.schema.fields(indexCol).name
      val indexData = data.select(indexColName).rdd.distinct()
        .map { case Row(index: String) => index }.filter(a => a != null).collect()

      val outputArray = new Array[sql.DataFrame](indexData.length)
      var indexForOutput = 0
      for (index_i <- indexData) {
        val dataTmp = data.where(s"$indexColName == $index_i")
        val modelTmp = sc.objectFile[ARIMAModel](modelPath + s"/$index_i").first()
        val returnDF = predictOnce(dataTmp, timeColName, timeSeriesColName, nFuture, modelTmp)
        outputArray(indexForOutput) = returnDF.withColumn("index", lit(index_i))
        indexForOutput += 1

      }
      val outputUnion = outputArray.toSeq.reduce(_ union _).repartition(partitionNum)
      DataSaver.save(outputUnion, output, sep)
    }
    else {
      val modelTmp = sc.objectFile[ARIMAModel](modelPath).first()
      val returnDF = predictOnce(data, timeColName, timeSeriesColName, nFuture, modelTmp)
      DataSaver.save(returnDF, output, sep)
    }

  }

  def predictOnce(data: sql.DataFrame,
                  timeColName: String,
                  timeSeriesColName: String,
                  nFuture: Int,
                  model: ARIMAModel): sql.DataFrame = {

    val outputDF = model
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)
      .setNFuture(nFuture)
      .transformWithConfidence(data)

    outputDF
  }

}
