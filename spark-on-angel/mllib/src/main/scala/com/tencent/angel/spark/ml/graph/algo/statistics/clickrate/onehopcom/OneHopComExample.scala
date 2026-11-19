package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehopcom

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object OneHopComExample {
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val mode = "local"
    val input = "data/bc/clickedges"
    val output = "model/oneHopCom/"
    val extraInput = Array("data/bc/usercom", "data/bc/userad")
    val delimiter = " "
    val partitionNum = 2
    val psPartitionNum = 2
    val storageLevel = StorageLevel.MEMORY_ONLY_SER
    
    start(mode)
    
    val oneHopCom = new OneHopCom()
      .setDelimiter(delimiter)
      .setExtraInputs(extraInput)
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psPartitionNum)
      .setStorageLevel(storageLevel)
    
    val df = GraphIO.load(input, isWeighted = false)
    val mapping = oneHopCom.transform(df)
    println("process cost is " + (System.currentTimeMillis() - t1) * 1.0f / 1000)
    GraphIO.save(mapping, output)
    println("cost time is " + (System.currentTimeMillis() - t1) * 1.0f / 1000)
    
    
    stop()
  }
  
  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("onehopcom")
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
