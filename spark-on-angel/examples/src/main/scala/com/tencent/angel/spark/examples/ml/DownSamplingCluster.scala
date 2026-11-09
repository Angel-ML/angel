package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.featurePreprocess.DataSampling.DownSampling
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}

object DownSamplingCluster {
  def main(args: Array[String]): Unit = {
    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val labelCol = params.getOrElse("labelCol", null).toInt
    val targetLabel = params.getOrElse("targetLabel", null)
    val sampledLabel = params.getOrElse("sampledLabel", null)
    val sampleRatio = params.getOrElse("sampleRatio", "1.0").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)
    val dataset = DataLoader.loadTable(sc, input, partitionNum, 1, "", sep)
    val dataCols = dataset.columns
    val labelColName = dataCols(labelCol)

    val DownSampling = new DownSampling()
      .setSampleRatio(sampleRatio)
      .setLabelCol(labelColName)
      .setTargetLabel(targetLabel)
      .setSampledLabel(sampledLabel)

    val result = DownSampling.transform(dataset)
    DataSaver.save(result, output, sep)
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