package com.tencent.angel.spark.ml.classification.KNN

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.classification.knn.{KNNClassificationModel, KNNClassifier}
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object KNNLocal {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    val mode = "local"

    val input = "data/mlTestData/Iris.csv"
    val labelCol = 4
    val featureCols = "0-3"
    val sep = ","
    val partitionNum = 1
    val sampleRate = 1

    val actionType = "predict" //"predict"//"train"
    val modelPath = "data/output/modeLlout"
    val output = "data/output/output"

    val k = 4
    val topTreeSize = 10
    val topTreeLeafSize = 5
    val subTreeLeafSize = 5
    val balanceThreshold = 0
    val outputType = "separate" //separate，merge

    val sc = start(mode)

    actionType match {
      case "train" =>
        train(sc, input, featureCols, labelCol,
          k, topTreeSize, topTreeLeafSize, subTreeLeafSize, balanceThreshold, partitionNum, sampleRate, sep, modelPath)
      case "predict" =>
        val model = sc.objectFile[KNNClassificationModel](modelPath).first()
        predict(sc, input, output, featureCols, model, partitionNum, sampleRate, sep, outputType)

      case _ =>
        println(s"$actionType do not support")
    }
    stop()

  }

  def train(sc: SparkContext,
            input: String,
            featureColExpr: String,
            labelCol: Int,
            k: Int,
            topTreeSize: Int,
            topTreeLeafSize: Int,
            subTreeLeafSize: Int,
            balanceThreshold: Double,
            partitionNum: Int,
            sampleRate: Double,
            sep: String,
            modelPath: String): Unit = {

    val data = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureColExpr, labelCol, false, sep)
    println(s"Train data size: ${data.count()}")

    val knnModel = new KNNClassifier()
      .setLabelCol("label")
      .setFeaturesCol("feature")
      .setK(k)
      .setTopTreeSize(topTreeSize)
      .setTopTreeLeafSize(topTreeLeafSize)
      .setSubTreeLeafSize(subTreeLeafSize)
      .setBalanceThreshold(balanceThreshold)
      .fit(data)

    val outputPath = new Path(modelPath)
    val conf = sc.hadoopConfiguration
    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    sc.parallelize(Seq(knnModel), 1).saveAsObjectFile(modelPath)

  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              featureColExpr: String,
              model: KNNClassificationModel,
              partitionNum: Int,
              sampleRate: Double,
              sep: String,
              outputType: String): Unit = {

    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)
    var resultDF = model.transform(data).drop("feature").drop("rawPrediction")
    resultDF.show(2)
    if (outputType == "separate") {
      resultDF = new VectorDisassembler()
        .setInputCol("probability").transform(resultDF).drop("probability")
        .withColumn("predictionNew", col("prediction")).drop("prediction")
    }

    DataSaver.save(resultDF, output, sep)
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}
