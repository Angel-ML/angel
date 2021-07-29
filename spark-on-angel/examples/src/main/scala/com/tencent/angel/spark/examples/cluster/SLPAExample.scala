package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.community.slpa.SLPA
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SLPAExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")

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
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)

    val slpa = new SLPA()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setNumMaxCommunities(numMaxCommunities)
      .setBatchSize(batchSize)
      .setMaxIteration(maxIteration)
      .setPreserveRate(preserveRate)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)
      .setNeedReplicaEdge(needReplicateEdge)
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
