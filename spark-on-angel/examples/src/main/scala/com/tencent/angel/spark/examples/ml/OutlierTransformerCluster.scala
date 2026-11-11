package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.classification.OutlierTransformer.OutlierTransformer
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}

object OutlierTransformerCluster {
  def main(args: Array[String]): Unit = {
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

    val featureCol = params.getOrElse("featureCol", "0").toInt
    val method = params.getOrElse("method", "zScore")
    val upperPercentage = params.getOrElse("upperPercentage", "0.5").toDouble
    val lowerPercentage = params.getOrElse("lowerPercentage", "0.0").toDouble
    val upperThreshold = params.getOrElse("upperThreshold", "1.0").toDouble
    val lowerThreshold = params.getOrElse("lowerThreshold", "0.0").toDouble
    val softenOutlier = params.getOrElse("softenOutlier", "true").toBoolean


    val sc = start(mode)

    run(sc, input, output, featureCol, method, upperPercentage,
      lowerPercentage, upperThreshold, lowerThreshold, softenOutlier, partitionNum, sampleRate, sep)

    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          featureCol: Int,
          method: String,
          upperPercentage: Double,
          lowerPercentage: Double,
          upperThreshold: Double,
          lowerThreshold: Double,
          softenOutlier: Boolean,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {


    val data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)
    val colName = data.schema.fields(featureCol).name

    val outlierTransformer = new OutlierTransformer()
      .setColNames(Array(colName))
      .setMethod(method)
      .setSoftenOutlier(softenOutlier)
      .setUpperPercentage(upperPercentage)
      .setLowerPercentage(lowerPercentage)
      .setUpperThreshold(upperThreshold)
      .setLowerThreshold(lowerThreshold)

    val resultDF = outlierTransformer.transform(data)
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
