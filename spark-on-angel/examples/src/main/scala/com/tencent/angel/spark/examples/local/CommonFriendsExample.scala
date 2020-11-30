package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.statistics.commonfriends.CommonFriends
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CommonFriendsExample {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/commonfriends"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val pullBatchSize = 1000
    val enableCheck = true
    val bufferSize = 100000
    val psPartitionNum = 2
    val extraInput = input
    val isCompressed = false
    val compressIndex = 2
    val srcIndex = 0
    val dstIndex = 1
    val sep = " "

    start(mode)
    val commonfriends = new CommonFriends()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setDebugMode(enableCheck)
      .setBufferSize(bufferSize)
      .setIsCompressed(isCompressed)
      .setPSPartitionNum(psPartitionNum)
      .setInput(input)
      .setExtraInputs(Array(extraInput))
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)
      .setCompressIndex(compressIndex)
      .setDelimiter(sep)

    val df = GraphIO.load(input, isWeighted = false)
    val mapping = commonfriends.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("commonfriends")
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
