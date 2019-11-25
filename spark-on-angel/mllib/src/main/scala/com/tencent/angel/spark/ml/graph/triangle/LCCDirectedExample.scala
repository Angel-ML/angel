package com.tencent.angel.spark.ml.graph.triangle

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LCCDirectedExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/test/edge-dir"
    val sep = Delimiter.parse(Delimiter.SPACE)
    val output = "model/lcc_dir"
    val partitionNum = 2
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 5000
    val pullBatchSize = 1000
    val enableCheck = true
    val bufferSize = 100000
    val psPartitionNum = 1
    val srcIndex = 0
    val dstIndex = 1

    start(mode)
    val startTime = System.currentTimeMillis()
    val lcc = new LCCDirected()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setDebugMode(enableCheck)
      .setBufferSize(bufferSize)
      .setPSPartitionNum(psPartitionNum)
      .setInput(input)
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)

    val df = GraphIO.load(input, isWeighted = false, sep = sep)
    val mapping = lcc.transform(df)
    GraphIO.save(mapping, output)

    println(s"${System.currentTimeMillis() - startTime} ms elapsed")
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("lcc_directed")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    sc.setLogLevel("WARN")
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
