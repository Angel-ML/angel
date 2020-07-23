package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.rank.pagerank.vertexcut.PageRank
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object PageRankExample {
  def main(args: Array[String]): Unit = {
    val input = "data/bc/edge"
    val output = "model/pagerank"
    val partitionNum = 3
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 2

    start()
    val pagerank = new PageRank()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setApproNodeNum(10000)
      .setTol(0.01f)
      .setResetProb(0.15f)
    val df = GraphIO.load(input, isWeighted = false)
    val mapping = pagerank.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("pagerank")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("cp")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
