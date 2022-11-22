package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.statistics.hyperloglog.HyperVertices
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HyperVertexExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = "local"
    val input = "data/bc/karate_club_network.txt"
    val inputV = null
    val output = "data/output/hyperVertex"
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
    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val withVertexType = false
    val sep = " "
    val sepV = " "
    val tags = "0,1"
    val vertexIndex = params.getOrElse("vertexIndex", "0").toInt
    val typeIndex = params.getOrElse("typeIndex", "1").toInt
    val isSaveCounter = params.getOrElse("isSaveCounter", "false").toBoolean

    start(mode)
    val hyperVertices = new HyperVertices()
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
      .setWithVertexType(withVertexType)
      .setBalancePartitionPercent(percent)
      .setSaveCounter(isSaveCounter)

    val df = GraphIO.load(input, isWeighted = isWeight,
      srcIndex = 0, dstIndex = 1,
      weightIndex = 2, sep = sep)

    val nodeType = if (withVertexType) {
      GraphIO.loadVertexType(inputV, vertexIndex, typeIndex, sepV)
    } else {
      null.asInstanceOf[DataFrame]
    }

    val tagSet = if (withVertexType) {
      tags.split(",").toSet
    } else {
      null.asInstanceOf[Set[String]]
    }

    hyperVertices.transform(df, nodeType, tagSet, output)
    //GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hyperVertex")
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