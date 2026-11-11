package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver, LogUtils}
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

/**
 * GaussianMixture聚类算法
 *
 * 参数说明:
 *
 * mode          "local" or yarn
 * input         input file
 * featureCols      表示需要计算的特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数
 * partitionNum   partition数目
 * sampleRate   采样率. 默认:1.0 [optional]
 * output        聚类结果输出路径
 * centersOutput 聚类中心输出路径
 *
 * k             k-means聚类个数k
 * maxIter       最大迭代次数. 默认:20 [optional]
 */


object GaussianMixtureCluster {

  def main(args: Array[String]) {
    LogUtils.logTime("GaussianMixture clustering start...")

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val actionType = params.getOrElse("actionType", "train")
    val modelPath = params.getOrElse("modelPath", null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val k = params.getOrElse("K", "3").toInt
    val maxIteration = params.getOrElse("maxIter", "20").toInt
    val needClusteringEvaluator = params.getOrElse("needClusteringEvaluator", "true").toBoolean
    val epsilon = params.getOrElse("tol", "1e-4").toDouble
    val outputType = params.getOrElse("outputType", "merge") //separate，merge

    val sc = start(mode)

    actionType match {
      case "train" =>
        train(sc, input, featureCols, partitionNum, sampleRate, k, maxIteration, epsilon, sep, modelPath, needClusteringEvaluator, output, outputType)
      case "predict" =>
        val model = GaussianMixtureModel.load(modelPath)
        predict(sc, input, output, featureCols, model, partitionNum, sampleRate, sep, outputType)
      case _ =>
        println(s"$actionType do not support")
    }
    stop()


  }

  def train(sc: SparkContext, input: String,
            featureCols: String,
            partitionNum: Int,
            sampleRate: Double,
            k: Int,
            maxIteration: Int,
            epsilon: Double,
            sep: String,
            modelPath: String,
            needClusteringEvaluator: Boolean,
            output: String,
            outputType: String
           ): Unit = {

    val inputDF = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)
    println(s"Train data size: ${inputDF.count()}")

    val gmModel = new GaussianMixture()
      .setK(k)
      .setMaxIter(maxIteration)
      .setFeaturesCol(DFStruct.FEATURE)
      .setTol(epsilon)
      .fit(inputDF)

    var resultDF = gmModel.transform(inputDF).cache()

    if (needClusteringEvaluator) {
      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator()
        .setFeaturesCol(DFStruct.FEATURE)
        .setMetricName("silhouette")

      val silhouette = evaluator.evaluate(resultDF)
      println(s"Silhouette with squared euclidean distance = $silhouette")
    }

    gmModel.write.overwrite().save(modelPath)
    resultDF = resultDF.drop(DFStruct.FEATURE)
    if (outputType == "separate") {
      resultDF = new VectorDisassembler()
        .setInputCol("probability").transform(resultDF).drop("probability")
    }
    resultDF = resultDF.withColumn("predictionNew", col("prediction")).drop("prediction")
    DataSaver.save(resultDF, output, sep)
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              featureColExpr: String,
              model: GaussianMixtureModel,
              partitionNum: Int,
              sampleRate: Double,
              sep: String,
              outputType: String): Unit = {
    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)
    var resultDF = model.transform(data).drop("feature")

    if (outputType == "separate") {
      resultDF = new VectorDisassembler()
        .setInputCol("probability").transform(resultDF).drop("probability")
    }
    resultDF = resultDF.withColumn("predictionNew", col("prediction")).drop("prediction")
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
