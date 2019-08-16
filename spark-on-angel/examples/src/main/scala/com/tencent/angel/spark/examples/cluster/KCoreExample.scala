package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.kcore5.KCore
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KCoreExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }



    val kCore = new KCore()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    val mapping = kCore.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    // Add jvm parameters for executors
    var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
    //executorJvmOptions += " -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
    //  "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
    //conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    println(s"executorJvmOptions = ${executorJvmOptions}")

    conf.setMaster(mode)
    conf.setAppName("K-Core")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
