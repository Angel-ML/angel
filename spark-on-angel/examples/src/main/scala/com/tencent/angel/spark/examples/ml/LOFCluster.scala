package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.ml.outlier.LOF
import org.apache.spark.{SparkConf, SparkContext}


object LOFCluster {
  def main(args: Array[String]) {
    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val minPts = params.getOrElse("minPts", "5").toInt
    val featureCols = params.getOrElse("featureCols", null)

    val sc = start(mode)
    run(sc, input, output, featureCols, minPts, partitionNum, sampleRate, sep)
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
    val lof = new LOF()
      .setFeaturesCol(DFStruct.FEATURE)
      .setMinPts(minPts)

    var resultDF = lof.transform(data)
    resultDF = new VectorDisassembler()
      .setInputCol(DFStruct.FEATURE).transform(resultDF).drop(DFStruct.FEATURE)
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
