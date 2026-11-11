package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.cliques.kcliqueV2.KClique
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object KCliqueV2Example {
  def main(args: Array[String]): Unit = {
    
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val sc = start(mode)
    
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val maxK = params.getOrElse("maxK", "4").toInt
    val batchSize = params.getOrElse("batchSize", "1000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "10000").toInt
    
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
    
    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    
    val kcl = new KClique()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setMaxK(maxK)
      .setPullBatchSize(pullBatchSize)
    
    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(kcl.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = kcl.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }
  
  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    
    // Add jvm parameters for executors
    var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
    executorJvmOptions += " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    println(s"executorJvmOptions = ${executorJvmOptions}")
    
    conf.setMaster(mode)
    conf.setAppName("K-Clique")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }
  
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
