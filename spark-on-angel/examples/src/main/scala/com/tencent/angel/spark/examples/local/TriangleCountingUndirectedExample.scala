package com.tencent.angel.spark.examples.local

import com.tencent.angel.graph.statistics.triangle.TriangleCountingUndirected
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object TriangleCountingUndirectedExample {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/triangle"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val psPartitionNum = 1
    val pullBatchSize = 1000
    val computeLCC = false
    val sep = " "

    start(mode)
    val startTime = System.currentTimeMillis()
    val triangleCount = new TriangleCountingUndirected()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setPSPartitionNum(psPartitionNum)
      .setComputeLcc(computeLCC)


    val df = GraphIO.load(input, isWeighted = false, sep = sep)
    val mapping = triangleCount.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("triangleCounting_undirected")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
