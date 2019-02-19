package com.tencent.angel.spark.examples.cluster

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.kcore.KCore

object KCoreExample {

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    if(sc.isLocal) {
//      sc.setLogLevel("WARN")
    }
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "1000").toInt
    val switchRate = params.getOrElse("switchRate", "0.001").toDouble
    val mode = params.getOrElse("mode", "yarn-cluster")
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY_SER"))

    start(mode)
    val edges = SparkContext.getOrCreate().textFile(input, partitionNum).flatMap { line =>
      val arr = line.split("[\\s+,]")
      val src = arr(0).toInt
      val dst = arr(1).toInt
      if (src >= 0 && dst >= 0) Iterator((src, dst), (dst, src)) else Iterator.empty
    }

    val kCore = KCore.process(edges, partitionNum, None, storageLevel, switchRate)

    // save
    kCore.map { case (id, core) => s"$id\t$core"}.saveAsTextFile(output)

    stop()
  }

}
