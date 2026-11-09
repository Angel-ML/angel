package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver, ParamKeys}
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorDisassembler}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

object PolynomialExpansionCluster {
  def main(args: Array[String]) {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val featureCols = params.getOrElse("featureCols", null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val degree = params.getOrElse("degree", "2").toInt


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
