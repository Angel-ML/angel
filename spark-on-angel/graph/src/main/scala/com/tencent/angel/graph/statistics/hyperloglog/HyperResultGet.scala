package com.tencent.angel.graph.statistics.hyperloglog

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import scala.collection.mutable.ArrayBuffer

object HyperResultGet {

  def processANF(model: HyperANFPSModel, numBatch: Int, keys: Array[Long]): (Long, Iterator[(Long, Long, Long)]) = {
    var graphPartANF: Long = 0
    val nodeANFs = new ArrayBuffer[(Long, Long, Long)](keys.length)
    val batchSize = math.max(keys.length / numBatch, 100)
    var start = 0
    while (start < keys.length) {
      val subKeys = keys.slice(start, math.min(start + keys.length, start + batchSize))
      graphPartANF += batchProcessANF(model, subKeys, nodeANFs)
      start += batchSize
    }
    (graphPartANF, nodeANFs.iterator)
  }

  def batchProcessANF(model: HyperANFPSModel, keys: Array[Long], nodeANFs: ArrayBuffer[(Long, Long, Long)]): Long = {
    var batchGraphANF: Long = 0
    if (keys == null || keys.length == 0) return 0
    val writeCounters = model.getWriteCounter(keys)
    if (writeCounters.size() == 0) return 0
    for (idx <- keys) {
      batchGraphANF += writeCounters.get(idx)._1.cardinality()
      nodeANFs.append((idx, writeCounters.get(idx)._1.cardinality(), writeCounters.get(idx)._2))
    }
    batchGraphANF
  }

  def processANFCounter(model: HyperANFPSModel, numBatch: Int, keys: Array[Long]): (Long, Iterator[(Long, Long, Long, HyperLogLogPlus)]) = {
    var graphPartANF: Long = 0
    val nodeANFs = new ArrayBuffer[(Long, Long, Long, HyperLogLogPlus)](keys.length)
    val batchSize = math.max(keys.length / numBatch, 100)
    var start = 0
    while (start < keys.length) {
      val subKeys = keys.slice(start, math.min(start + keys.length, start + batchSize))
      graphPartANF += batchProcessANFCounter(model, subKeys, nodeANFs)
      start += batchSize
    }
    (graphPartANF, nodeANFs.iterator)
  }

  def batchProcessANFCounter(model: HyperANFPSModel, keys: Array[Long], nodeANFs: ArrayBuffer[(Long, Long, Long, HyperLogLogPlus)]): Long = {
    var batchGraphANF: Long = 0
    if (keys == null || keys.length == 0) return 0
    val writeCounters = model.getWriteCounter(keys)
    if (writeCounters.size() == 0) return 0
    for (idx <- keys) {
      batchGraphANF += writeCounters.get(idx)._1.cardinality()
      nodeANFs.append((idx, writeCounters.get(idx)._1.cardinality(), writeCounters.get(idx)._2, writeCounters.get(idx)._1 ))
    }
    batchGraphANF
  }

}
