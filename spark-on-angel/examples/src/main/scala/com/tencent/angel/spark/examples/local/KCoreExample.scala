package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.kcore.KCore
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KCoreExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/kcore"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val psPartitionNum = 2

    start(mode)
    val kCore = new KCore()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPSPartitionNum(psPartitionNum)

    val df = GraphIO.load(input, isWeighted = false)
    val mapping = kCore.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
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
