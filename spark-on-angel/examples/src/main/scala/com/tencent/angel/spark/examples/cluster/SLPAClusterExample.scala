package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.communitydetection.slpa.SLPA
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SLPAClusterExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val weightIndex = params.getOrElse("weightCol", "2").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "40").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val useEdgeBalancePartition = params.getOrElse("useEdgeBalancePartition", "false").toBoolean
    val numMaxCommunities = params.getOrElse("numMaxCommunities", "5").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val needReplicateEdge = params.getOrElse("needReplicateEdge", "true").toBoolean
    val preserveRate = params.getOrElse("preserveRate", "0.1").toFloat
    val maxIteration = params.getOrElse("maxIteration", "10").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val needOutputCommunityProbability = params.getOrElse("needOutputCommunityProbability", "false").toBoolean

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    val sc = start(mode)

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val slpa = new SLPA()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setWeightCol("weight")
      .setBatchSize(batchSize)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setNeedReplicaEdge(needReplicateEdge)
      .setNumMaxCommunities(numMaxCommunities)
      .setPreserveRate(preserveRate)
      .setMaxIteration(maxIteration)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)
      .setUseBalancePartition(useBalancePartition)
      .setNeedOutputCommunityProbability(needOutputCommunityProbability)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(slpa.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = slpa.transform(df)

    GraphIO.save(mapping, output)

    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("slpa")
    val sc = new SparkContext(conf)
//    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
