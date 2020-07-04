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
    val data = params.getOrElse("data", "edge")
    val mode = "local"
    val input = "data/bc/" + data
    val output = "model/cc"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 1
    val srcIndex = 0
    val dstIndex = 1
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
