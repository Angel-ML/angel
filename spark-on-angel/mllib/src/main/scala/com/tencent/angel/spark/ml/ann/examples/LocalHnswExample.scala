package com.tencent.angel.spark.ml.ann.examples

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.ann.algo.Hnsw
import com.tencent.angel.spark.ml.ann.utils.DataUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object LocalHnswExample {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val vectorPath = "data/embeddingData/embTest"
    val outputPath = "model/ann/out"
    val queryPath = "data/embeddingData/oneQuery"
    
    start(mode)
    val hnsw = new Hnsw()
      .setTopK(10)
      .setStorageLevel("MEMORY_ONLY")
      .setM(16)
      .setEfConstruction(500)
      .setEf(500)
      .setPartitionNum(2)
      .setQueryPartitionNum(100)
      .setPSPartitionNum(2)
      .setIsInternal(false)
      .setQueryPath(queryPath)
      .setDisFunc("l2-distance")
      .setVecSep(" ")
      .setItemSep(":")
      .setSheetsNum(5)
    
    val ss = SparkSession.builder().getOrCreate()
    val df = DataUtils.loadVectors(ss, vectorPath, 2, itemSep = ":", vecSep = " ")
    
    val retDF = hnsw.transform(df)
    
    retDF.show(10)
    
    println(s"start to save")
    
    DataUtils.saveResult(retDF, outputPath, sep = "\t")
    
    stop()
  }
  
  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hnsw")
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
