package com.tencent.angel.spark.ml.featurePreprocess.DropDuplicates

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}

object DropDuplicatesLocal {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/mlTestData/evaluation_two1.txt"
    val output = "data/output/output"
    val partitionNum = 1
    val sampleRate = 1
    val sep = " "

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {

    val inputDF = DataLoader.loadByLine(sc, input, partitionNum, sampleRate)
    val resultDF = inputDF.distinct().repartition(partitionNum)
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
