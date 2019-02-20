package com.tencent.angel.spark.examples.local

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.kcore.{KCore, KCoreGraphPartition}

object KCoreExample {

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {
    val switchRate = 0.1
    val mode = "local"
    val input = "F:\\data\\amazon0601\\amazon0601.txt\\amazon0601_u.txt"
    val output = "model/kcore"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY

    start(mode)
    val graph = SparkContext.getOrCreate().textFile(input, partitionNum).flatMap { line =>
      val arr = line.split("[\\s+,]")
      val src = arr(0).toInt
      val dst = arr(1).toInt
      if (src != dst) Iterator((src, dst), (dst, src)) else Iterator.empty
    }.groupByKey(partitionNum).mapPartitions { iter =>
      val keys = new ArrayBuffer[Int]()
      val values = new ArrayBuffer[Array[Int]]()
      iter.foreach { case (key, group) =>
        keys += key
        values += group.toSet.toArray
      }
      Iterator.single((keys.toArray, values.toArray))
    }.map { case (keys, values) =>
      KCoreGraphPartition(keys, values)
    }.persist(storageLevel)

    val kCore = KCore.process(graph, partitionNum, None, storageLevel, switchRate)

    // save
    kCore.map { case (id, core) =>
      s"$id\t$core"
    }.saveAsTextFile(output)

    stop()
  }

}
