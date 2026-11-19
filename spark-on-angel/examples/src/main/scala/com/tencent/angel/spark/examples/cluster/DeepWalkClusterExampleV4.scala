package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.embedding.deepwalkNoWeight.DeepWalkNoWeight
import com.tencent.angel.spark.ml.graph.algo.embedding.deepwalkWithWeight.DeepWalk
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object DeepWalkClusterExampleV4 {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")

    val input = params.getOrElse("input", null)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val weightIndex = params.getOrElse("weightCol", "2").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "40").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val useEdgeBalancePartition = params.getOrElse("useEdgeBalancePartition", "false").toBoolean
    val useBalancePartition = params.getOrElse("useBalancePartition", "true").toBoolean
    val walkLength = params.getOrElse("walkLength", "10").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val needReplicateEdge = params.getOrElse("needReplicateEdge", "true").toBoolean
    val dynamicInitNeighbor = params.getOrElse("dynamicInitNeighbor", "false").toBoolean
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val numWalks = params.getOrElse("numWalks", "3").toInt


    val sc = start(mode)

    if (isWeighted) {

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
        .setEpochNum(numWalks)
        .setDynamicInitNeighbor(dynamicInitNeighbor)

      deepwalk.setOutputDir(output)
      val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)

      //collect app name and data size
      DataStatistics.setAppName(deepwalk.getClass.getCanonicalName)
      DataStatistics.calcRDDSize(df, "angel.input.data")
      DataStatistics.setDataPartitionNum(partitionNum)
      DataStatistics.setPSPartitionNum(psPartitionNum)

      val mapping = deepwalk.transform(df)

    }
    else {

      val deepwalk = new DeepWalkNoWeight()
        .setStorageLevel(storageLevel)
        .setPSPartitionNum(psPartitionNum)
        .setSrcNodeIdCol("src")
        .setDstNodeIdCol("dst")
        .setBatchSize(batchSize)
        .setWalkLength(walkLength)
        .setPartitionNum(partitionNum)
        .setNeedReplicaEdge(needReplicateEdge)
        .setUseEdgeBalancePartition(useEdgeBalancePartition)
        .setEpochNum(numWalks)
        .setDynamicInitNeighbor(dynamicInitNeighbor)
      deepwalk.setOutputDir(output)
      val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)

      //collect app name and data size
      DataStatistics.setAppName(deepwalk.getClass.getCanonicalName)
      DataStatistics.calcRDDSize(df, "angel.input.data")
      DataStatistics.setDataPartitionNum(partitionNum)
      DataStatistics.setPSPartitionNum(psPartitionNum)

      val mapping = deepwalk.transform(df)

    }

    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("deepwalk")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
