package com.tencent.angel.spark.ml.classification.OutlierTransformer

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}


object OutlierTransformerLocal {
  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/mlTestData/example_air_passengers.csv"
    val output = "data/output/outLR"
    val partitionNum = 1
    val sep = ","
    val sampleRate = 1
    val featureCol =  1
    val method =  "zScore"
    val upperPercentage = 0.5
    val lowerPercentage = 0.0
    val upperThreshold = 3.0
    val lowerThreshold =0.0
    val softenOutlier = false

    val sc = start(mode)

    run(sc, input, output, featureCol, method, upperPercentage,
        lowerPercentage, upperThreshold, lowerThreshold, softenOutlier,partitionNum, sampleRate, sep)

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
