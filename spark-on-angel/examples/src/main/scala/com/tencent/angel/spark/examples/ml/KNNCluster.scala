package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, MAX_INT_STR, ParamKeys}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.classification.knn.{KNNClassificationModel, KNNClassifier}
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object KNNCluster {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val k = params.getOrElse("K", "4").toInt
    val topTreeSize = params.getOrElse("topTreeSize", "10").toInt
    val topTreeLeafSize = params.getOrElse("topTreeLeafSize", "5").toInt
    val subTreeLeafSize = params.getOrElse("subTreeLeafSize", "5").toInt
    val balanceThreshold = params.getOrElse("balanceThreshold", "0").toDouble
    val actionType = params.getOrElse("actionType", "train")
    val modelPath = params.getOrElse("modelPath", null)
    val outputType = params.getOrElse("outputType", "merge") //separate，merge

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
