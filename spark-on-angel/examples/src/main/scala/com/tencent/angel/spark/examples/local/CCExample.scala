package com.tencent.angel.spark.examples.local

import com.tencent.angel.graph.connectedcomponent.wcc.WCC
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.graph.utils.Delimiter

object CCExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "data/bc/edge")
    val mode = params.getOrElse("mode", "local")
    val output = params.getOrElse("output", "model/cc")
    val partitionNum = params.getOrElse("partitionNum", "2").toInt
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = params.getOrElse("psPartitionNum", "2").toInt
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val useBalancePartition = false


    val sep = Delimiter.parse(params.getOrElse("sep",Delimiter.SPACE))
    start(mode)

    val cc = new WCC()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    val mapping = cc.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("CC")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
