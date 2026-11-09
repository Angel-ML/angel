package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainDensity.LouvainDensity
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainDensityClusterExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val weightIndex = params.getOrElse("weightCol", "2").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean

    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "40").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt

    val numFold = params.getOrElse("numFold", "5").toInt
    val numOpt = params.getOrElse("numOpt", "10").toInt
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val eps = params.getOrElse("eps", "0.0001").toFloat
    val bufferSize = params.getOrElse("bufferSize", "10000").toInt
    val preserveRate = params.getOrElse("preserveRate", "0.1").toFloat
    val useMergeStrategy  = params.getOrElse("useMergeStrategy", "true").toBoolean


    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    val sc = start(mode)

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val louvain = new LouvainDensity()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setNumFold(numFold)
      .setNumOpt(numOpt)
      .setBatchSize(batchSize)
      .setDebugMode(enableCheck)
      .setEps(eps)
      .setBufferSize(bufferSize)
      .setIsWeighted(isWeighted)
      .setPSPartitionNum(psPartitionNum)
      .setPreserveRate(preserveRate)
      .setUseMergeStrategy(useMergeStrategy)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(louvain.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)
    PSContext.getOrCreate(sc)

    val mapping = louvain.transform(df)

    GraphIO.save(mapping, output)

    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("LouvainDensity")
    val sc = new SparkContext(conf)
//    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
