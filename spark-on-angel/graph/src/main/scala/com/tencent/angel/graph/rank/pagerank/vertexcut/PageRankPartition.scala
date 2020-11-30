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

package com.tencent.angel.graph.rank.pagerank.vertexcut

import com.tencent.angel.graph.utils.LongArrayListIndexComparator
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrays
import it.unimi.dsi.fastutil.longs.{Long2FloatOpenHashMap, LongArrayList, LongOpenHashSet}
import java.util.{Arrays => JArrays}

private[vertexcut]
class PageRankPartition(index: Int,
                        srcs: Array[Long],
                        dsts: Array[Long],
                        sums: Array[Float],
                        weights: Array[Float],
                        keys: Array[Long]) extends Serializable {
  assert(srcs.length == dsts.length)

  def getIndex: Int = index

  def start(model: PageRankPSModel, rank: Float, resetProb: Float, tol: Float): Int = {
    val outMsgs = new Long2FloatOpenHashMap()
    for (idx <- srcs.indices) {
      val delta = rank
      if (delta > tol)
        outMsgs.addTo(dsts(idx), delta * weights(idx) / sums(idx))
    }

    val update = VFactory.sparseLongKeyFloatVector(model.dim)
    update.setStorage(new LongFloatSparseVectorStorage(update.dim(), outMsgs))
    model.sendMsgs(update)
    outMsgs.size()
  }

  def process(model: PageRankPSModel, resetProb: Float, tol: Float, numMsgs: Long): Int = {
    val outMsgs = new Long2FloatOpenHashMap()

    val inMsgs = model.readMsgs(keys)
    for (idx <- srcs.indices) {
      val delta = inMsgs.get(srcs(idx)) * (1 - resetProb)
      if (delta > tol)
        outMsgs.addTo(dsts(idx), delta * weights(idx) / sums(idx))
    }

    val update = VFactory.sparseLongKeyFloatVector(model.dim)
    update.setStorage(new LongFloatSparseVectorStorage(update.dim(), outMsgs))
    model.sendMsgs(update)

    outMsgs.size()
  }

  def process(keys: Array[Long], vals: Array[Float], outMsgs: Long2FloatOpenHashMap): Unit = {
    var i = 0
    var j = 0
    while (i < keys.length && j < srcs.length) {
      while (i < keys.length && keys(i) < srcs(j)) i += 1
      while (i < keys.length && j < srcs.length && keys(i) > srcs(j)) j += 1
      // keys(i) == srcs(j)
      if (i < keys.length && j < srcs.length) {
        outMsgs.addTo(dsts(j), vals(i) * weights(j))
        j += 1
      }
    }
  }

  def setMissRanks(model: PageRankPSModel, initRanks: Float): Int = {
    if (srcs.length > 0) {
      val ranks = model.readRanks(keys)
      val update = ranks.emptyLike()
      for (idx <- srcs.indices) {
        if (ranks.get(srcs(idx)) == 0.0)
          update.set(srcs(idx), initRanks)
      }
      model.updateRanks(update)
      update.size().toInt
    } else 0
  }

  def calcWeightSums(model: PageRankPSModel): Int = {
    val startMerge = System.currentTimeMillis()
    val update = new Long2FloatOpenHashMap(keys.length)
    for (idx <- srcs.indices)
      update.addTo(srcs(idx), weights(idx))
    val mergeTime = System.currentTimeMillis() - startMerge

    val start = System.currentTimeMillis()
    val msgs = VFactory.sparseLongKeyFloatVector(model.dim)
    msgs.setStorage(new LongFloatSparseVectorStorage(msgs.dim(), update))
    model.updateSums(msgs)
    val updateTime = System.currentTimeMillis() - start
    println(s"mergeTime=${mergeTime} updateTime=${updateTime}")
    msgs.size().toInt
  }

  def toPartitionWithSum(model: PageRankPSModel): PageRankPartition = {
    val msgs = model.readSums(keys)
    val sums = new Array[Float](srcs.length)
    for (idx <- srcs.indices)
      sums(idx) = msgs.get(srcs(idx))

    new PageRankPartition(index, srcs, dsts, sums, weights, keys)
  }
}

private[vertexcut]
object PageRankPartition {
  def apply(index: Int, iterator: Iterator[(Long, Long, Float)]): PageRankPartition = {
    val srcs = new LongArrayList()
    val dsts = new LongArrayList()
    val weights = new FloatArrayList()
    val keys = new LongOpenHashSet()
    while (iterator.hasNext) {
      val entry = iterator.next()
      srcs.add(entry._1)
      dsts.add(entry._2)
      weights.add(entry._3)
      keys.add(entry._1)
    }
    new PageRankPartition(index,
      srcs.toLongArray(),
      dsts.toLongArray(),
      null,
      weights.toFloatArray(),
      keys.toLongArray())
  }

  def sortPartition(partId: Int,
                    srcs: LongArrayList,
                    dsts: LongArrayList,
                    weights: FloatArrayList,
                    keys: LongOpenHashSet): PageRankPartition = {
    val index = new Array[Int](srcs.size())
    for (i <- index.indices) index(i) = i
    IntArrays.quickSort(index, new LongArrayListIndexComparator(srcs))
    val srcsArray = new Array[Long](srcs.size())
    val dstsArray = new Array[Long](dsts.size())
    val weightsArray = new Array[Float](weights.size())
    for (i <- index.indices) {
      srcsArray(i) = srcs.getLong(index(i))
      dstsArray(i) = dsts.getLong(index(i))
      weightsArray(i) = weights.getFloat(index(i))
    }
    val keysArray = keys.toLongArray()
    JArrays.sort(keysArray)
    new PageRankPartition(partId, srcsArray, dstsArray,
      null,
      weightsArray, keysArray)
  }
}
