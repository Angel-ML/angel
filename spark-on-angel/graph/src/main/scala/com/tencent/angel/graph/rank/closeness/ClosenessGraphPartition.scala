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

private [closeness]
class ClosenessGraphPartition(index: Int,
                              keys: Array[Long],
                              indptr: Array[Int],
                              outNodes: Array[Long],
                              p: Int, sp: Int, seed: Long) {

  def init(model: ClosenessPSModel): Unit = {
    model.init((keys ++ outNodes).distinct, p, sp, seed)
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
    model.sendMsgs(outMsgs, p, sp, seed)
    outMsgs.size()
  }

  def save(model: ClosenessPSModel, numNodes: Long, isConnected: Boolean): (Array[Long], Array[Float]) = {
    val retMap = model.readCloseness(keys.clone(), numNodes, isConnected)
    (keys, keys.map(retMap.getOrDefault(_, 0.0).toFloat))
  }

  def saveClosenessAndCentrality(model: ClosenessPSModel, numNodes: Long,
                                 isDirected: Boolean, isConnected: Boolean): (Array[Long], Array[(JDouble, JLong, JDouble)]) = {
    val retMap = model.readClosenessAndCardinality(keys.clone(), numNodes, isDirected, isConnected)
    (keys, keys.map(retMap.getOrDefault(_, (0d, 0L, 0d))))
  }
}

private [closeness]
object ClosenessGraphPartition {
  def apply(index: Int, iter: Iterator[(Long, Iterable[Long])], p: Int, sp: Int, seed: Long): ClosenessGraphPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()

    indptr.add(0)
    var idx = 0
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, outs) = (entry._1, entry._2)
      outs.toArray.distinct.foreach {n => outNodes.add(n)}
      indptr.add(outNodes.size())
      keys.add(node)
      idx += 1
    }

    new ClosenessGraphPartition(index,
      keys.toLongArray(),
      indptr.toIntArray(),
      outNodes.toLongArray(),
      p, sp, seed)
  }
}