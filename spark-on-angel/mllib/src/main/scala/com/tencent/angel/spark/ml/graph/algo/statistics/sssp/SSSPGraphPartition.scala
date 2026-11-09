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
package com.tencent.angel.spark.ml.graph.algo.statistics.sssp

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

private[sssp]
class SSSPGraphPartition(index: Int,
                         keys: Array[Long],
                         indptr: Array[Int],
                         outNodes: Array[Long],
                         weights: Array[Float]) extends Serializable {

  def initMsgs(model: SSSPPSModel, source: Long): Int = {
    val msgs = VFactory.sparseLongKeyFloatVector(model.dim)
    for (idx <- keys.indices) {
      if (keys(idx) == source) {
        msgs.set(keys(idx), 0.0f)
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          if (weights(j) == 0.0f)
            throw new AngelException(s"cannot allow zero weights for source node ${source}")
          msgs.set(outNodes(j), weights(j))
          j += 1
        }
      }
    }

    println(s"msgs.size()=${msgs.size()}")
    if (msgs.size() > 0)
      model.updateMsgs(msgs)

    0
  }

  def process(model: SSSPPSModel, numMsgs: Long): Int = {
    val inMsgs = if (numMsgs < keys.length) model.readAllMsgs() else model.readMsgs(keys.clone())
    val outMsgs = VFactory.sparseLongKeyFloatVector(inMsgs.dim())

    for (idx <- keys.indices) {
      val d = inMsgs.get(keys(idx))
      if (d != 0.0f) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          if (outMsgs.hasKey(outNodes(j))) {
            val old = outMsgs.get(outNodes(j))
            outMsgs.set(outNodes(j), math.min(d + weights(j), old))
          } else
            outMsgs.set(outNodes(j), d + weights(j))
          j += 1
        }
      }
    }

    model.updateMsgs(outMsgs)
    outMsgs.size().toInt
  }

  def save(model: SSSPPSModel, partitionIds: Array[Int],
           ends: Array[Int]): (Array[Long], Array[Float]) = {
    val length = if (index > 0) ends(index) - ends(index - 1) else ends(0)
    val start = if (index > 0) ends(index - 1) else 0
    val myPartitionIds = new Array[Int](length)
    for (i <- start until ends(index))
      myPartitionIds(i - start) = partitionIds(i)

    var nodes = new Array[Long](0)
    if (length > 0) {
      nodes = model.getNodes(myPartitionIds)
      if (nodes.length > 0)
        (nodes, model.readDistances(nodes.clone()).get(nodes))
      else
        (nodes, new Array[Float](0))
    } else {
      (nodes, new Array[Float](0))
    }
  }

}

private[sssp]
object SSSPGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])]): SSSPGraphPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()
    val weights = new FloatArrayList()

    indptr.add(0)
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, outs) = (entry._1, entry._2)
      outs.foreach { case (n, w) =>
        outNodes.add(n)
        weights.add(w)
      }
      indptr.add(outNodes.size())
      keys.add(node)
    }

    new SSSPGraphPartition(index,
      keys.toLongArray(),
      indptr.toIntArray(),
      outNodes.toLongArray(),
      weights.toFloatArray())
  }

  def apply(index: Int, keys: Array[Long], indptr: Array[Int],
            outNodes: Array[Long], weights: Array[Float]): SSSPGraphPartition = {
    new SSSPGraphPartition(index, keys, indptr,
      outNodes, weights)
  }
}
