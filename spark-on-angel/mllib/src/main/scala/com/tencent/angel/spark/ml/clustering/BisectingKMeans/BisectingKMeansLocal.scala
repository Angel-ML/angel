package com.tencent.angel.spark.ml.clustering.BisectingKMeans

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver, LogUtils}
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * BisectingKMeans聚类算法
 *
 * 参数说明:
 *
 * mode          "local" or yarn-cluster
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


object BisectingKMeansLocal {

  def main(args: Array[String]) {
    LogUtils.logTime("BisectingKMeans clustering start...")

    val mode = "local"
    val input = "data/mlTestData/Iris.csv"
    val featureCols = "0-3"
    val sampleRate = 1
    val partitionNum = 1
    val output = "data/output/outBKMeans"
    val centersOutput = "data/output/outBKMeansCenters"
    val modelPath = "data/output/BKMModel"
    // for k-means algorithm

    val k = 3
    val maxIteration = 20
    val distanceMeasure = "euclidean" //"euclidean","cosine"
    val minDivisibleClusterSize = 1

    val needClusteringEvaluator = true
    val sep = ","
    val actionType = "train" //"predict"//"train"
    val sc = start(mode)

    actionType match {
      case "train" =>
        train(sc, input, featureCols, partitionNum, sampleRate, k, maxIteration, distanceMeasure, minDivisibleClusterSize, sep, centersOutput, modelPath, needClusteringEvaluator, output)
      case "predict" =>
        val model = BisectingKMeansModel.load(modelPath)
        predict(sc, input, output, featureCols, model, partitionNum, sampleRate, sep)
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
            distanceMeasure: String,
            minDivisibleClusterSize: Int,
            sep: String,
            centersOutput: String,
            modelPath: String,
            needClusteringEvaluator: Boolean,
            output: String
           ): Unit = {

    val inputDF = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)
    println(s"Train data size: ${inputDF.count()}")

    val bkmModel = new BisectingKMeans()
      .setK(k)
      .setMaxIter(maxIteration)
      .setFeaturesCol(DFStruct.FEATURE)
      .setDistanceMeasure(distanceMeasure)
      .setMinDivisibleClusterSize(minDivisibleClusterSize)
      .fit(inputDF)

    var resultDF = bkmModel.transform(inputDF).cache()
    resultDF.show(3)

    if (centersOutput != null) {
      val centerRDD = sc.parallelize(bkmModel.clusterCenters.map(a => a.toArray.map(b => b.toString)))
      DataSaver.save(centerRDD, centersOutput, sep)
    }

    if (needClusteringEvaluator) {
      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator()
        .setFeaturesCol(DFStruct.FEATURE)
        .setMetricName("silhouette")

      val silhouette = evaluator.evaluate(resultDF)
      println(s"Silhouette with squared euclidean distance = $silhouette")
    }


    bkmModel.write.overwrite().save(modelPath)
    resultDF = resultDF.drop(DFStruct.FEATURE)
    DataSaver.save(resultDF, output, sep)
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              featureColExpr: String,
              model: BisectingKMeansModel,
              partitionNum: Int,
              sampleRate: Double,
              sep: String): Unit = {
    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)
    data.show(5)
    val resultDF = model.transform(data).drop("feature")
    resultDF.show(5)
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
