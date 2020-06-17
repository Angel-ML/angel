package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.rank.hindex.HIndex
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object HIndexExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/hindex"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 1

    start(mode)
    val hindex = new HIndex()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)

    val df = GraphIO.load(input, isWeighted = false, sep = " ")
    val mapping = hindex.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hindex")
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
