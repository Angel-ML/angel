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
package com.tencent.angel.spark.ml.graph.connectedcomponent.wcc

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongLongVector}
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

class WCCGraphPartition(index: Int,
                        keys: Array[Long],
                        indptr: Array[Int],
                        neighbors: Array[Long],
                        keyLabels: Array[Long],
                        indices: Array[Long]) extends Serializable {
  def initMsgs(model: WCCPSModel): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices){
      msgs.set(keys(i), keys(i))
    }
    model.initMsgs(msgs)
    msgs.size().toInt
  }
  
  // if label of node is larger than its neighbors',
  // change it into min among its neighbors' labels
  def process(model: WCCPSModel, numMsgs: Long, isFirstIteration: Boolean): Int = {
    var changedNum = 0
    if (numMsgs > indices.length || isFirstIteration) {
      val inMsgs = model.readMsgs(indices)
      val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())

      for (idx <- keys.indices) {
        keyLabels(idx) = inMsgs.get(keys(idx))
        val newLabel = minNbrLabel(idx, inMsgs)
        if (newLabel < keyLabels(idx)) {
          keyLabels(idx) = newLabel
          outMsgs.set(keys(idx), newLabel)
          changedNum += 1
        }
      }
      model.writeMsgs(outMsgs)
      changedNum
    }
    else {
      val inMsgs = model.readAllMsgs()
      val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())
      
      for (idx <- keys.indices) {
        val newLabel = minNbrLabel(idx, inMsgs)
        if (newLabel < keyLabels(idx)) {
          keyLabels(idx) = newLabel
          outMsgs.set(keys(idx), newLabel)
          changedNum += 1
        }
      }

      model.writeMsgs(outMsgs)
      changedNum
    }
  }
  
  def save(): (Array[Long], Array[Long]) = {
    (keys, keyLabels)
  }

  def minNbrLabel(idx: Int, inMsgs: LongLongVector): Long = {
    var j = indptr(idx)
    var minLabel = keyLabels(idx)
    while (j < indptr(idx + 1)) {
      val t = inMsgs.get(neighbors(j))
      if (minLabel > t) {
        minLabel = t
      }
      j += 1
    }
    minLabel

  }
}

object WCCGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): WCCGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()

    indptr.add(0)
    
    while (iterator.hasNext) {
      val (node, ns) = iterator.next()
      keys.add(node)
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      indptr.add(neighbors.size())
    }

    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    
    new WCCGraphPartition(index, keysArray, indptr.toIntArray(),
      neighborsArray, new Array[Long](keysArray.length), keysArray.union(neighborsArray).distinct)
  }
}
