package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.statistics.hyperloglog.HyperDecimal
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object HyperDecimalExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = "local"
    val input = "data/bc/HLL_test_data.txt"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val output = "data/output/results"
    val psPartitionNum = 1
    val srcIndex = 0
    val dstIndex = 1
    val infoIndex = 2
    val tagIndex = 3
    val withEdgeTag = true
    val tags = "0,1"
    val useBalancePartition = false
    val p = 16
    val maxIter = 2
    val msgNumBatch = 1
    val verboseSaving = false
    val isDirected = false
    val isInDegree = true
    val percent = 0.7f
    val sep = "\t"

    start(mode)
    val hyperDecimal = new HyperDecimal()
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

    val df = GraphIO.loadStringInfoLabel(input, withEdgeTag, srcIndex, dstIndex, infoIndex, tagIndex, sep)

    val tagSet = if (withEdgeTag) {
      tags.split(",").toSet
    } else {
      null.asInstanceOf[Set[String]]
    }

    var startTime = System.currentTimeMillis()
    hyperDecimal.transform(df, tagSet, output)
    println(s"finish transform task, total cost time: ${(System.currentTimeMillis()-startTime)/1000.0}s.")
    //startTime = System.currentTimeMillis()
    //GraphIO.save(mapping, output)
    //println(s"finish saving edge decimal results, cost time: ${(System.currentTimeMillis()-startTime)/1000.0}s.")
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hyperDecimal")
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