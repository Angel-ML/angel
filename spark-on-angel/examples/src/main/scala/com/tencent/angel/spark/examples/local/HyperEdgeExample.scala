package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.statistics.hyperloglog.HyperEdges
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HyperEdgeExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = "local"
    val input = "data/bc/karate_club_network.txt"
    val output = "data/output/hyperEdge"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 1
    val useBalancePartition = false
    val isInDegree = true
    val percent = 0.7F
    val p = params.getOrElse("p", "12").toInt
    val maxIter = 2
    val msgNumBatch = params.getOrElse("msgNumBatch", "1").toInt
    val verboseSaving = params.getOrElse("verboseSaving", "false").toBoolean
    val isDirected = params.getOrElse("isDirected", "true").toBoolean
    val withEdgeTag = false
    val sep = " "
    val tags = "1,2"

    start(mode)
    val hyperEdges = new HyperEdges()
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psPartitionNum)
      .setStorageLevel(storageLevel)
      .setP(p)
      .setMaxIter(maxIter)
      .setUseBalancePartition(useBalancePartition)
      .setMsgNumBatch(msgNumBatch)
      .setVerboseSaving(verboseSaving)
      .setIsDirected(isDirected)
      .setIsInDegree(isInDegree)
      .setWithEdgeTag(withEdgeTag)
      .setBalancePartitionPercent(percent)


    val df = GraphIO.loadStringWeight(input, isWeighted = withEdgeTag,
      srcIndex = 0, dstIndex = 1,
      weightIndex = 2, sep = sep)

    val tagSet = if (withEdgeTag) {
      tags.split(",").toSet
    } else {
      null.asInstanceOf[Set[String]]
    }

    hyperEdges.transform(df, tagSet, output)
    //GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hyperEdge")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}