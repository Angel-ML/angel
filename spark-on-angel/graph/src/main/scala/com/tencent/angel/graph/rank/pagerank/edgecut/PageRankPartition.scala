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

package com.tencent.angel.graph.rank.pagerank.edgecut

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage
import com.tencent.angel.ml.math2.vector.LongFloatVector
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2FloatOpenHashMap, LongArrayList}

private[edgecut]
class PageRankPartition(index: Int,
                        keys: Array[Long], indptr: Array[Int],
                        sums: Array[Float],
                        outNodes: Array[Long],
                        weights: Array[Float]) {

  assert(keys.length == indptr.length - 1)

  def getIndex: Int = index

  def start(model: PageRankPSModel, rank: Float, resetProb: Float, tol: Float): Int = {
    val outMsgs = new Long2FloatOpenHashMap()
    for (idx <- keys.indices) {
      val delta = rank
      if (delta > tol) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          outMsgs.addTo(outNodes(j), delta * weights(j) / sums(idx))
          j += 1
        }
      }
    }

    val update = VFactory.sparseLongKeyFloatVector(model.dim)
    update.setStorage(new LongFloatSparseVectorStorage(update.dim(), outMsgs))
    model.sendMsgs(update)
    outMsgs.size()
  }

  def process(model: PageRankPSModel, resetProb: Float, tol: Float, numMsgs: Long): Int = {
    var inMsgs: LongFloatVector = null
    val outMsgs = new Long2FloatOpenHashMap()
    inMsgs = model.readMsgs(keys.clone())
    for (idx <- keys.indices) {
      val delta = inMsgs.get(keys(idx)) * (1 - resetProb)
      if (delta > tol) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          outMsgs.addTo(outNodes(j), delta * weights(j) / sums(idx))
          j += 1
        }
      }
    }

    inMsgs.setStorage(new LongFloatSparseVectorStorage(inMsgs.dim(), outMsgs))
    model.sendMsgs(inMsgs)
    outMsgs.size()
  }

  def setMissRanks(model: PageRankPSModel, initRanks: Float): Int = {
    if (keys.length > 0) {
      val ranks = model.readRanks(keys.clone())
      val update = ranks.emptyLike()
      for (idx <- keys.indices) {
        if (ranks.get(keys(idx)) == 0.0)
          update.set(keys(idx), initRanks)
      }
      model.updateRanks(update)
      update.size().toInt
    } else 0
  }

}

private[edgecut] object PageRankPartition {
  def apply(index: Int, iter: Iterator[(Long, Iterable[(Long, Float)])]): PageRankPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()
    val sums = new FloatArrayList()
    val weights = new FloatArrayList()

    indptr.add(0)
    var idx = 0
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, outs) = (entry._1, entry._2)
      outs.foreach { case (n, weight) =>
        outNodes.add(n)
        weights.add(weight)
      }
      indptr.add(outNodes.size())
      keys.add(node)
      sums.add(outs.map(f => f._2).sum)
      idx += 1
    }

    new PageRankPartition(index,
      keys.toLongArray(),
      indptr.toIntArray(),
      sums.toFloatArray(),
      outNodes.toLongArray(),
      weights.toFloatArray())
  }
}
