package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.community.slpa.SLPA
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.graph.utils.GraphIO

object SLPAExample {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/karate_club_network.txt"
    val storageLevel = StorageLevel.fromString("MEMORY_ONLY")
    val batchSize = 50
    val output = "data/output/outTmp"
    val srcIndex = 0
    val dstIndex = 1
    val weightIndex = 2
    val psPartitionNum = 1
    val partitionNum = 1
    val useEdgeBalancePartition = false
    val isWeighted = false
    val arrayBoundsPath = null
    val numMaxCommunities = 5
    val preserveRate = 0.1f
    val needReplicaEdge = false
    val sep = " "
    val maxIteration = 10
    val useBalancePartition = true
    start(mode)

    val slpa = new SLPA()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setNumMaxCommunities(numMaxCommunities)
      .setBatchSize(batchSize)
      .setMaxIteration(maxIteration)
      .setPreserveRate(preserveRate)
      .setArrayBoundsPath(arrayBoundsPath)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)
      .setNeedReplicaEdge(needReplicaEdge)
      .setUseBalancePartition(useBalancePartition)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val mapping = slpa.transform(df)

    GraphIO.save(mapping, output)


    stop()
  }

  def start(mode: String = "local[4]"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("SLPA")
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
