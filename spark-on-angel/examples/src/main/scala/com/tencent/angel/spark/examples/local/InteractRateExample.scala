package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.feature.interactRate.InteractRate
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object InteractRateExample {


  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/dEdge"
    val output = "model/angel/interactRate"
    val isWeighted = true
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val psPartitionNum = 1

    start(mode)
    val interactRate_ = new InteractRate()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setIsWeighted(isWeighted)

    val df = GraphIO.load(input, isWeighted = true)
    val mapping = interactRate_.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("interactRate")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
