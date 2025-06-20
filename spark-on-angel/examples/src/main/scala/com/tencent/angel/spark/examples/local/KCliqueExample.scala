package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.kclique.KClique
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KCliqueExample {
  
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/kclique/"
    
    val partitionNum = 2
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 2
    val maxK = 4
    
    start(mode)
    val kcl = new KClique()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setMaxK(maxK)

    val df = GraphIO.load(input, isWeighted = false)
    val mapping = kcl.transform(df)
    println("process cost is " + (System.currentTimeMillis() - t1) * 1.0f / 1000)
    GraphIO.save(mapping, output)
    println("cost time is " + (System.currentTimeMillis() - t1) * 1.0f / 1000)
    
    stop()
  }
  
  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("cc")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    conf.set("spark.hadoop.angel.ps.router.type", "range")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
  }
  
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
  
}
