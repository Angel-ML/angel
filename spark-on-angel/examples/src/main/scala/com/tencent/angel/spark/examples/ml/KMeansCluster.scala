package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util._
import org.apache.spark.ml.linalg.{Vector => mlVector}
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

/**
 * KMeans聚类算法
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
 * initMode      k-means聚类中心初始化模式. 默认:k-means|| [optional:"random" or "k-means||"]
 * maxIter       最大迭代次数. 默认:20 [optional]
 * epsilon       最小终止条件:累计误差. 默认:1e-4 [optional]
 */


object KMeansCluster extends {
  val K_KEY = "k"
  val INIT_MODE_KEY = "initMode"

  def main(args: Array[String]) {
    LogUtils.logTime("KMeans clustering start...")
    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val partitionNum = params.getOrElse("partitionNum", "4").toInt
    val output = params.getOrElse("output", null)
    val centersOutput = params.getOrElse("centersOutput", null)

    // for k-means algorithm
    val k = params.getOrElse("K", "3").toInt
    val initMode = params.getOrElse(INIT_MODE_KEY, MLlibKMeans.K_MEANS_PARALLEL)  // "random" or "k-means||"
    val maxIteration = params.getOrElse("maxIter", "20").toInt
    val epsilon = params.getOrElse("tol", "1e-4").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val clusterResult = run(input,
      featureCols,
      partitionNum,
      k,
      sampleRate,
      initMode,
      maxIteration,
      epsilon,
      sc,
      sep,
      centersOutput)
    DataSaver.save(clusterResult, output, sep)
  }

  /**
   * 调用聚类算法
   *
   * @param input
   * @param partitionNum
   * @param k
   * @param sampleRate
   * @param initMode
   * @param maxIteration
   * @param epsilon
   * @param sc
   * @return
   */
  def run(input: String,
          featureCols: String,
          partitionNum: Int,
          k: Int,
          sampleRate: Double,
          initMode: String,
          maxIteration: Int,
          epsilon: Double,
          sc: SparkContext,
          sep: String,
          centersOutput: String): DataFrame = {

    val inputDF = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)
    val inputRDD = inputDF.select(DFStruct.FEATURE).rdd.map { case Row(feat: mlVector) => Vectors.fromML(feat) }

    val km = new MLlibKMeans()
      .setK(k)
      .setInitializationMode(initMode)
      .setMaxIterations(maxIteration)
      .setEpsilon(epsilon)

    val kmModel = km.run(inputRDD)

    LogUtils.logTime("KMeans Cluster center:")
    for (center <- kmModel.clusterCenters) {
      LogUtils.logTime(center.toString)
    }

    val centerRDD = sc.parallelize(kmModel.clusterCenters.map(a => a.toArray.map(a => a.toString)))
    if (centersOutput != null) {
      DataSaver.save(centerRDD, centersOutput, sep)
    }
    val predictUDF = udf { (features: Any) =>
      kmModel.predict(Vectors.fromML(features.asInstanceOf[mlVector]))
    }
    inputDF.withColumn(DFStruct.PREDICT, predictUDF(col(DFStruct.FEATURE))).drop(DFStruct.FEATURE)
  }
}
