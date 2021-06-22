package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}


object DataSamplingExample {
  def main(args: Array[String]): Unit = {
    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val takeSample = params.get("takeSample").map(_.toLong)
    val featureCols = params.getOrElse("featureCols", null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

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
