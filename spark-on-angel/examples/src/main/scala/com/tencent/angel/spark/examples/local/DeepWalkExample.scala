package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.embedding.deepwalk.DeepWalk
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object DeepWalkExample {
  def main(args: Array[String]): Unit = {
    val input = "data/bc/karate_club_network.txt"
    val storageLevel = StorageLevel.fromString("MEMORY_ONLY")
    val batchSize = 10
    val output = "data/output/output1"
    val srcIndex = 0
    val dstIndex = 1
    val weightIndex = 2
    val psPartitionNum = 1
    val partitionNum = 1
    val useEdgeBalancePartition = false
    val isWeighted = false
    val needReplicateEdge =true

    val sep = " "
    val walkLength = 10


    start()

    val deepwalk = new DeepWalk()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setWeightCol("weight")
      .setBatchSize(batchSize)
      .setWalkLength(walkLength)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setNeedReplicaEdge(needReplicateEdge)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)
      .setEpochNum(2)

    deepwalk.setOutputDir(output)
    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val mapping = deepwalk.transform(df)

    stop()
  }

  def start(mode: String = "local[4]"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("DeepWalk")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("data/cp")
    //PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
