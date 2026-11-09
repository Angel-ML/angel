package com.tencent.angel.spark.ml.featurePreprocess.PolynomialExpansion

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorDisassembler}
import org.apache.spark.{SparkConf, SparkContext}

object PolynomialExpansionLocal {
  def main(args: Array[String]) {
    val mode = "local"
    val input = "data/mlTestData/Dummy_data_test.txt"
    val featureCols = "0-1"
    val sep = " "
    val output = "data/output/outVI"
    val partitionNum = 1
    val sampleRate = 1
    val degree = 2


    val sc = start(mode)
    run(sc, input, output, featureCols, degree, partitionNum, sampleRate, sep)
    stop()
  }

  private def run(sc: SparkContext,
                  input: String,
                  output: String,
                  featureCols: String,
                  degree: Int,
                  partitionNum: Int,
                  sampleRate: Double,
                  sep: String): Unit = {

    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)

    val polynomialExpansion = new PolynomialExpansion()
      .setDegree(degree)
      .setInputCol(DFStruct.FEATURE)
      .setOutputCol("polyFeatures")

    var resultDF = polynomialExpansion.transform(data).drop(DFStruct.FEATURE)

    resultDF = new VectorDisassembler()
      .setInputCol("polyFeatures")
      .transform(resultDF).drop("polyFeatures")

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
