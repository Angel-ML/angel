package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.rank.closeness.Closeness
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object ClosenessExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val data = params.getOrElse("data", "edge")
    val mode = "local"
    val input = "data/bc/" + data
    val output = "model/hyperanf"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val psPartitionNum = 1
    val p = params.getOrElse("p", "6").toInt
    val msgNumBatch = params.getOrElse("msgNumBatch", "8").toInt
    val verboseSaving = params.getOrElse("verboseSaving", "true").toBoolean
    val isDirected = params.getOrElse("isDirected", "true").toBoolean

    start(mode)
    val closeness = new Closeness()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPSPartitionNum(psPartitionNum)
      .setP(p)
      .setMsgNumBatch(msgNumBatch)
      .setVerboseSaving(verboseSaving)
      .setIsDirected(isDirected)

    val df = GraphIO.load(input, isWeighted = false)
    val mapping = closeness.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("closeness")
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
