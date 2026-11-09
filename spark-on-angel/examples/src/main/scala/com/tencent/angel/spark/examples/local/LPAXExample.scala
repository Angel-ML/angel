package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.communitydetection.lpax.LPAX
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object LPAXExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/bipartiteEdge"
    val flagPath = "data/bc/nodeFlag"
    val output = "model/angel/lpax"
    val isWeighted = true
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val psPartitionNum = 2

    start(mode)
    val lpax = new LPAX()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setIsWeighted(isWeighted)

    val df = GraphIO.load(input, isWeighted = true)
    
    val dfAttr = GraphIO.load(flagPath, isWeighted = false)
    val nodeFlags = dfAttr.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1).toInt))
    
    lpax.setNodeFlag(nodeFlags)
    val mapping = lpax.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("lpax")
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
