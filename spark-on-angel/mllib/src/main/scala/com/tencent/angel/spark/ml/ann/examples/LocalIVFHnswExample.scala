package com.tencent.angel.spark.ml.ann.examples

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.ann.algo.IVFHnsw
import com.tencent.angel.spark.ml.ann.utils.DataUtils
import org.apache.spark.{SparkConf, SparkContext}

object LocalIVFHnswExample {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    var vectorPath = "data/output/outputCenter/*"
    val outputPath = "model/ann/out"
    var queryPath = "data/output/outputLevel2/*"

    if (vectorPath.endsWith("/")) vectorPath = vectorPath + "*"
    else if (vectorPath.endsWith("/*")) null
    else vectorPath = vectorPath + "/*"

    if (queryPath.endsWith("/")) queryPath = queryPath + "*"
    else if (queryPath.endsWith("/*")) null
    else queryPath = queryPath + "/*"

    val sc = start(mode)
    val ivfhnsw = new IVFHnsw()
      .setTopK(2)
      .setStorageLevel("MEMORY_ONLY")
      .setM(16)
      .setEfConstruction(500)
      .setEf(500)
      .setPartitionNum(2)
      .setQueryPartitionNum(1)
      .setPSPartitionNum(2)
      .setIsInternal(true)
      .setQueryPath(queryPath)
      .setDisFunc("l2-distance")
      .setVecSep(" ")
      .setItemSep(":")
      .setSheetsNum(2)
      .setBatchSize(5)
      .setTopKInCluster(40)
    //.setClusterIdx(2)


    val df = DataUtils.loadVectorsMultilevel(sc, vectorPath, 2, itemSep = ":", vecSep = " ")

    df.show(5)

    val retDF = ivfhnsw.transform(df).cache()
    println("retDF", retDF.count())
    retDF.show(10)

    println(s"start to save")
    DataUtils.saveResult(retDF, outputPath, sep = "\t")
    stop()
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hnsw")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }


}
