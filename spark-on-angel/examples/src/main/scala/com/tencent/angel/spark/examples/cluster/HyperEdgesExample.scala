package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.statistics.hyperloglog.HyperEdges
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object HyperEdgesExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", "")
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val output = params.getOrElse("output", null)
    val sc = start(mode)
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val tagIndex = params.getOrElse("tagIndex", "2").toInt
    val withEdgeTag = params.getOrElse("withEdgeTag", "false").toBoolean
    val tags = params.getOrElse("tags", "")
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val p = params.getOrElse("p", "6").toInt
    val maxIter = params.getOrElse("maxIter", "1").toInt
    val msgNumBatch = params.getOrElse("msgNumBatch", "20").toInt
    val verboseSaving = params.getOrElse("verboseSaving", "false").toBoolean
    val isDirected = params.getOrElse("isDirected", "true").toBoolean
    val isInDegree = params.getOrElse("isInDegree", "true").toBoolean
    val percent = params.getOrElse("balancePartitionPercent", "0.7").toFloat
    val isSaveCounter = params.getOrElse("isSaveCounter", "false").toBoolean

    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

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
      .setSaveCounter(isSaveCounter)

    val df = GraphIO.loadStringWeight(input, isWeighted = withEdgeTag,
      srcIndex = srcIndex, dstIndex = dstIndex,
      weightIndex = tagIndex, sep = sep)

    val tagSet = if (withEdgeTag) {
      tags.split(",").toSet
    } else {
      null.asInstanceOf[Set[String]]
    }
    //collect app name and data size

    PSContext.getOrCreate(sc)

    var startTime = System.currentTimeMillis()
    hyperEdges.transform(df, tagSet, output)
    println(s"finish transform task, total cost time: ${(System.currentTimeMillis()-startTime)/1000.0}s.")
    //startTime = System.currentTimeMillis()
    //GraphIO.save(mapping, output)
    //println(s"finish saving edge ANF results, cost time: ${(System.currentTimeMillis()-startTime)/1000.0}s.")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("AngelHyperEdges")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    //    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}