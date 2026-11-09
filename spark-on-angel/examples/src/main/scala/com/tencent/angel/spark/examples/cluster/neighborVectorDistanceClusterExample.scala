package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.feature.neighborVectorDistance.neighborVectorDistance
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object neighborVectorDistanceClusterExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt


    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "40").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val queryPartitionNum = params.getOrElse("queryPartitionNum", "10").toInt

    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val balancePartitionPercent = params.getOrElse("balancePartitionPercent", "0.7").toFloat

    val itemIdx = params.getOrElse("itemIdx", "0").toInt
    val vecIdx = params.getOrElse("vecIdx", "1").toInt
    val queryPath = params.getOrElse("queryPath", null)
    val distanceFuncion = params.getOrElse("distanceFunction", "cosine-distance")

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val itemSep = params.getOrElse("itemSep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
    }
    val vecSep = params.getOrElse("vecSep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
    }

    val sc = start(mode)

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val neighborVectorDistance = new neighborVectorDistance()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPSPartitionNum(psPartitionNum)
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setQueryPartitionNum(queryPartitionNum)
      .setQueryPath(queryPath)
      .setVecSep(vecSep)
      .setItemSep(itemSep)
      .setDisFunc(distanceFuncion)
      .setItemIdx(itemIdx)
      .setVecIdx(vecIdx)


    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, -1, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(neighborVectorDistance.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)
    PSContext.getOrCreate(sc)

    val mapping = neighborVectorDistance.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("neighborVectorDistanceClusterExample")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
