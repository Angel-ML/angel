package com.tencent.angel.spark.examples.cluster

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.kcore.{KCore, KCoreGraphPartition, ReIndex}

object KCoreExample {

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
    val sc = new SparkContext(conf)
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
    val enableReIndex = params.getOrElse("enableReIndex", "true").toBoolean
    val mode = params.getOrElse("mode", "yarn-cluster")
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY_SER"))

    start(mode)

    if (enableReIndex) {
      val adjs = SparkContext.getOrCreate().textFile(input, partitionNum).flatMap { line =>
        val arr = line.split("[\\s+,]")
        val src = arr(0).toLong
        val dst = arr(1).toLong
        if (src != dst) Iterator((src, dst), (dst, src)) else Iterator.empty
      }.groupByKey(partitionNum).mapPartitions { iter =>
        val keys = new ArrayBuffer[Long]()
        val values = new ArrayBuffer[Array[Long]]()
        iter.foreach { case (key, group) =>
          keys += key
          values += group.toSet.toArray
        }
        Iterator.single((keys.toArray, values.toArray))
      }.persist(storageLevel)

      val numNodes = adjs.map(_._1.length).aggregate(0L)(_ + _, _ + _)
      assert(numNodes < Int.MaxValue)
      val reIndexer = new ReIndex(numNodes)

      // train indexer
      adjs.flatMap(_._1).zipWithIndex().foreachPartition { pair =>
        reIndexer.train(pair)
      }

      val graph = adjs.map { case (keys, values) =>
        reIndexer.encode(keys, values)
      }.map { case (keys, values) =>
        KCoreGraphPartition(keys, values)
      }.persist(storageLevel)

      graph.foreachPartition(_ => Unit)
      adjs.unpersist(false)

      val kCore = KCore.process(graph, partitionNum, Some(numNodes.toInt), storageLevel, switchRate)

      // decode
      val result = kCore.flatMap { case (keys, cores) =>
        reIndexer.decode(keys).zip(cores).map { case (key, core) =>
          s"$key\t$core"
        }
      }
      // save
      result.saveAsTextFile(output)
    } else {
      val graph = SparkContext.getOrCreate().textFile(input, partitionNum).flatMap { line =>
        val arr = line.split("[\\s+,]")
        val src = arr(0).toInt
        val dst = arr(1).toInt
        if (src >= 0 && dst >= 0) Iterator((src, dst), (dst, src)) else Iterator.empty
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
      kCore.flatMap { case (ids, cores) =>
        ids.zip(cores).map { case (id, core) =>
          s"$id\t$core"
        }
      }.saveAsTextFile(output)
    }

    stop()
  }
}
