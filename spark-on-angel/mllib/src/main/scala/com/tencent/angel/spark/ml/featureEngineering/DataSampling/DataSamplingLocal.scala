package com.tencent.angel.spark.ml.featureEngineering.DataSampling

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils}
import org.apache.spark.{SparkConf, SparkContext}

object DataSamplingLocal {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/mlDataTest/wine.data"
    val output = "data/output/output"
    val partitionNum = 1
    val sampleRate = 0.5
    val takeSample = Option(21l)
    val featureCols = ""
    val sep = " "

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, takeSample, featureCols, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          takeSampleOpt: Option[Long],
          featureCols: String,
          sep: String): Unit = {
    takeSampleOpt.fold {
      val rdd = DataLoader.loadTable(sc, input, partitionNum, sampleRate, featureCols, sep).rdd
      DataSaver.save(rdd.map(r => r.toSeq.map(i => if (i == null) "" else i.toString).toArray), output, sep)
    } { takeSample =>
      val inputRDD = DataLoader.loadTable(sc, input, partitionNum, sampleRate, featureCols, sep).cache()
      val inputCount = inputRDD.count()
      if (inputCount != 0) {
        LogUtils.logTime(s"data size remained after first sampling: $inputCount")
        if (takeSample < inputCount) {
          val finalSampleRate = takeSample / inputCount.toDouble

          val outputRDD = inputRDD.sample(false, finalSampleRate)
          DataSaver.save(outputRDD.rdd.map(r => r.toSeq.map(_.toString).toArray), output, sep)
        }
      } else {
        LogUtils.logTime(s"Input data size: 0")
      }
    }
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
