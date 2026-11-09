package com.tencent.angel.spark.examples.local

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.statistics.triangle.WeightedMotifCountingV3
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object WeightedMotifCountV3LocalExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val input = params.getOrElse("input", "data/bc/dEdge")
    val output = params.getOrElse("output", "model/weightedmotifcountv3")
    val threshold = params.getOrElse("threshold", "100000").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "20").toInt

    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt

    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "DISK_ONLY"))
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val bufferSize = params.getOrElse("bufferSize", "1000000").toInt

    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val isWeighted = params.getOrElse("isWeighted", "true").toBoolean
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean

    val cpDir = "cp"
    val sc = start(mode)
    sc.setCheckpointDir(cpDir)

    val startTime = System.currentTimeMillis()
    val motifCount = new WeightedMotifCountingV3()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setDebugMode(enableCheck)
      .setBufferSize(bufferSize)
      .setPSPartitionNum(psPartitionNum)
      .setInput(input)
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)
      .setDelimiter(sep)
      .setSuperItemThreshold(threshold)
      .setIsWeighted(isWeighted)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex = srcIndex,
      dstIndex = dstIndex, weightIndex = weightIndex, sep = sep)

    //collect app name and data size

    val mapping = motifCount.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("WeightedMotifCountingDirectedV3")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
