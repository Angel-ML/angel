/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.rank.closeness

import java.lang.{Double => JDouble, Long => JLong}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}

private[closeness]
class ClosenessPartition(index: Int,
                         keys: Array[Long],
                         indptr: Array[Int],
                         outNodes: Array[Long],
                         p: Int, sp: Int) {

  def init(model: ClosenessPSModel): Unit = {
    model.init(keys.clone(), p, sp)
  }

  def process(model: ClosenessPSModel, numBatch: Int): Long = {
    var numMsgs = 0L
    val batchSize = math.max(keys.length / numBatch, 100)
    var start = 0
    while (start < keys.length) {
      numMsgs += batchProcess(model, start, math.min(keys.length, start + batchSize))
      start += batchSize
    }
    numMsgs
  }

  def batchProcess(model: ClosenessPSModel, start: Int, end: Int): Long = {
    if (start == end) return 0
    val outMsgs = new Long2ObjectOpenHashMap[HyperLogLogPlus]()
    val inMsgs = model.getHyperLogLog(keys.slice(start, end))
    if (inMsgs.size() == 0) return 0
    for (idx <- start until end) {
      if (inMsgs.containsKey(keys(idx))) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          val outNode = outNodes(j)
          val hll = outMsgs.getOrDefault(outNode, new HyperLogLogPlus(p, sp))
          outMsgs.put(outNode, hll.merge(inMsgs.get(keys(idx))).asInstanceOf[HyperLogLogPlus])
          j += 1
        }
      }
    }
    model.sendMsgs(outMsgs, p, sp)
    outMsgs.size()
  }

  def save(model: ClosenessPSModel, partitionIds: Array[Int],
           ends: Array[Int], numNodes: Long): (Array[Long], Array[Float]) = {
    val length = if (index > 0) ends(index) - ends(index - 1) else ends(0)
    val start = if (index > 0) ends(index - 1) else 0
    val myPartitionIds = new Array[Int](length)
    for (i <- start until ends(index))
      myPartitionIds(i - start) = partitionIds(i)

    var nodes = new Array[Long](0)
    if (length > 0) {
      nodes = model.getNodes(myPartitionIds)
      if (nodes.length > 0) {
        val retMap = model.readCloseness(nodes.clone(), numNodes)
        (nodes, nodes.map(retMap.getOrDefault(_, 0.0).toFloat))
      }
      else
        (nodes, new Array[Float](0))
    } else {
      (nodes, new Array[Float](0))
    }
  }

  def saveClosenessAndCentrality(model: ClosenessPSModel, partitionIds: Array[Int],
                                 ends: Array[Int], numNodes: Long, isDirected: Boolean): (Array[Long], Array[(JDouble, JLong, JLong)]) = {
    val length = if (index > 0) ends(index) - ends(index - 1) else ends(0)
    val start = if (index > 0) ends(index - 1) else 0
    val myPartitionIds = new Array[Int](length)
    for (i <- start until ends(index))
      myPartitionIds(i - start) = partitionIds(i)

    var nodes = new Array[Long](0)
    if (length > 0) {
      nodes = model.getNodes(myPartitionIds)
      if (nodes.length > 0) {
        val retMap = model.readClosenessAndCardinality(nodes.clone(), numNodes, isDirected)
        (nodes, nodes.map(retMap.getOrDefault(_, (0d, 0L, 0L))))
      }
      else
        (nodes, new Array[(JDouble, JLong, JLong)](0))
    } else {
      (nodes, new Array[(JDouble, JLong, JLong)](0))
    }
  }
}

private[closeness]
object ClosenessPartition {
  def apply(index: Int, iter: Iterator[(Long, Iterable[Long])], p: Int, sp: Int): ClosenessPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()

    indptr.add(0)
    var idx = 0
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, outs) = (entry._1, entry._2)
      outs.toArray.distinct.foreach { n => outNodes.add(n) }
      indptr.add(outNodes.size())
      keys.add(node)
      idx += 1
    }

    new ClosenessPartition(index,
      keys.toLongArray(),
      indptr.toIntArray(),
      outNodes.toLongArray(),
      p, sp)
  }
}