package com.tencent.angel.spark.ml.fpm.examples

import com.tencent.angel.spark.ml.fpm.dataset.Dataset
import com.tencent.angel.spark.ml.fpm.fpgrowth.FPGrowth
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object FPGrowthExample {
  def main(args: Array[String]): Unit = {
    
    val mode = "local"
//    val input = "C:/Users/shichenggao/Desktop/kosarak.txt"
    val input = "C:/Users/shichenggao/Desktop/upa.csv"
    val output = "C:/Users/shichenggao/Desktop/testFP"
    val itemCol = "items"
    val confidence = 0.03f
    val support = 0.05f
    val partitionNum = 4
    val startTime = System.currentTimeMillis()
    start(mode)
    val (id2Item, dataset) = Dataset.loadDataItems(input, 2, sep = ",", reIndex = true)(SparkContext.getOrCreate())
    val fpg = new FPGrowth()
      .setItemCol(itemCol)
      .setMinSupport(support)
      .setMinConfidene(confidence)
      .setPartitionNum(partitionNum)
      .setStorageLevel(StorageLevel.MEMORY_ONLY)
    
    val model = fpg.fit(dataset, id2Item)
    
    val rules = model.generateRules()
    rules.show()
    
    val freqItem = model.generateFreqItems()
    freqItem.show()

    Dataset.save(freqItem, output + "_freqItems")
    Dataset.save(rules, output + "_rules")
    
    println(s"time cost: ${System.currentTimeMillis() - startTime} ms")
    
    stop()
  }
  
  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("fpgrowth")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
  }
  
  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
  
}
