package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.feature.neighborVectorDistance.neighborVectorDistance
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object neighborVectorDistanceLocalExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/graph/karate_club_network.txt"
    val output = "data/output/disOutput"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 50
    val isWeighted = false
    val psPartitionNum = 1
    val queryPath = "data/mlTestData/nodeDistance_test_data.txt"

    val distanceFuncion = "cosine-distance"
    val useBalancePartition = true
    val balancePartitionPercent = 0.7f

    val srcIndex = 0
    val dstIndex = 1
    val weightIndex = 2
    val sep = " "


    start(mode)

    val neighborVectorDistance = new neighborVectorDistance()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPSPartitionNum(psPartitionNum)
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setQueryPartitionNum(1)
      .setPSPartitionNum(1)
      .setQueryPath(queryPath)
      .setVecSep(" ")
      .setItemSep(":")
      .setDisFunc(distanceFuncion)


    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    //df.show(5)
    val mapping = neighborVectorDistance.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("nodeDistance")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("data/output/cp")
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
