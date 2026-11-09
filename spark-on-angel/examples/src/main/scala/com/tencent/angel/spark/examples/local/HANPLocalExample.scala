package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.communitydetection.hanp.HANP
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HANPLocalExample {

  def main(args: Array[String]): Unit = {
    val input = "data/bc/edge"
    val output = "model/hanp"
    val partitionNum = 2
    val psPartitionNum = 2

    start()
    val algo = new HANP()
      .setPartitionNum(partitionNum)
      .setStorageLevel(StorageLevel.MEMORY_ONLY)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setDelta(0.1f)
      .setMaxIteration(3)
      .setPreserveRate(0.1f)
      .setIsWeighted(false)
      .setUseBalancePartition(false)
    val df = GraphIO.load(input, isWeighted = false)
    val mapping = algo.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("hanp-local")
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
