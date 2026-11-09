package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.time_series.feature.TimeSeriesFeatures
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}

object TimeSeriesFeaturesCluster {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val output = params.getOrElse("output", null)
    val featureCols = params.getOrElse("featureCols", null)
    val maxBin = params.getOrElse("maxBin", "2").toInt
    val c3Lag = params.getOrElse("c3Lag", "2").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    run(sc, input, output, featureCols, sampleRate, sep, maxBin, c3Lag, partitionNum)
    SparkContext.getOrCreate().stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          featureColExpr: String,
          sampleRate: Double,
          sep: String,
          maxBin: Int,
          c3Lag: Int,
          partitionNum: Int
         ): Unit = {

    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)

    val timeSeriesFeatureExtractor = new TimeSeriesFeatures()
      .setC3Lag(c3Lag)
      .setMaxBin(maxBin)
      .setSeriesColName("feature")

    val resultDF = timeSeriesFeatureExtractor.transform(data.select("feature")).drop("feature")

    DataSaver.save(resultDF, output, sep)

  }
}
