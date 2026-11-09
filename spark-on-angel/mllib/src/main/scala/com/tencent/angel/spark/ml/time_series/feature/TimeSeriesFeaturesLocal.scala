package com.tencent.angel.spark.ml.time_series.feature

import com.tencent.angel.spark.ml.util._
import org.apache.spark.{SparkConf, SparkContext}

object TimeSeriesFeaturesLocal {
  def main(args: Array[String]): Unit = {
    val input = "data/mlTestData/series_time_feature_data"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outTSfeature"

    val featureCols = "0-3"
    val sampleRate = 1
    val maxBin = 2
    val c3Lag = 2
    val mode = "local"
    val partitionNum = 1
    val sep = ","

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
    data.show()
    val timeSeriesFeatureExtractor = new TimeSeriesFeatures()
      .setC3Lag(c3Lag)
      .setMaxBin(maxBin)
      .setSeriesColName("feature")

    val resultDF = timeSeriesFeatureExtractor.transform(data.select("feature")).drop("feature")

    DataSaver.save(resultDF, output, sep)

  }
}
