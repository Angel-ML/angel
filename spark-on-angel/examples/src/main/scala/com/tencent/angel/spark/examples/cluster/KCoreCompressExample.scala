package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.rank.kcore.KCoreCompress
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object KCoreCompressExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "5000").toInt
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val balancePartitionPercent = params.getOrElse("balancePartitionPercent", "0.7").toFloat
    val degreeThreshold = params.getOrElse("degreeThreshold", "10").toInt

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    val kCore = new KCoreCompress()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setTopK(degreeThreshold)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(kCore.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = kCore.transform(df)
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
    conf.setAppName("K-Core-Compress")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
