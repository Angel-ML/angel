package com.tencent.angel.spark.examples.local

import com.tencent.angel.graph.community.lpa.LPA
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object LPAExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val data = params.getOrElse("data", "edge")
    val mode = "local"
    val input = "data/bc/" + data
    val output = "model/lpa"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 1
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val maxIter = params.getOrElse("maxIter", "10").toInt
    val sc = start(mode)

    val sep = Delimiter.parse(params.getOrElse("sep",Delimiter.SPACE))


    val lpa = new LPA()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)
      .setMaxIter(maxIter)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    val mapping = lpa.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("LPA")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
