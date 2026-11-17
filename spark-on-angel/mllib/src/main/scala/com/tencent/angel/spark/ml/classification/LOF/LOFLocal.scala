package com.tencent.angel.spark.ml.classification.LOF

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.ml.outlier.LOF
import org.apache.spark.{SparkConf, SparkContext}


object LOFLocal{

  def main(args: Array[String]) {
    val mode = "local"
    val input = "data/mlTestData/anomaly_detection_X_train.csv"
    val partitionNum = 1
    val sampleRate = 1
    val output = "data/output/outLOF"
    val minPts = 5
    val featureCols = null
    val sep = ","

    val sc = start(mode)
    run(sc, input, output, featureCols, minPts,partitionNum,sampleRate,sep)
    stop()

  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          featureColExpr: String,
          minPts: Int,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {

    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)
    data.show(5)

    val lof = new LOF()
      .setFeaturesCol(DFStruct.FEATURE)
      .setMinPts(minPts)

    var resultDF = lof.transform(data)
    resultDF.show(5)

    resultDF = new VectorDisassembler()
      .setInputCol(DFStruct.FEATURE).transform(resultDF).drop(DFStruct.FEATURE)

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
