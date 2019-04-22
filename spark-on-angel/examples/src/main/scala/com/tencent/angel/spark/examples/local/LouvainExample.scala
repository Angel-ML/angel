package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.louvain.Louvain
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/louvain"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val numFold = 2
    val numOpt = 5
    val batchSize = 100
    val enableCheck = true
    val eps = 0.0
    val bufferSize = 100000
    val isWeighted = false
    val psPartitionNum = 2

    start(mode)
    val louvain = new Louvain()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setNumFold(numFold)
      .setNumOpt(numOpt)
      .setBatchSize(batchSize)
      .setDebugMode(enableCheck)
      .setEps(eps)
      .setBufferSize(bufferSize)
      .setIsWeighted(isWeighted)
      .setPSPartitionNum(psPartitionNum)
    val df = GraphIO.load(input, isWeighted = isWeighted)
    val mapping = louvain.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
