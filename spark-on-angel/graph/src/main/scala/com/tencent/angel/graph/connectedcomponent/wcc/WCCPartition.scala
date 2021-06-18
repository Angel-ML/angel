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
package com.tencent.angel.graph.connectedcomponent.wcc

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongLongVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

class WCCPartition(index: Int,
                              keys: Array[Long],
                              indptr: Array[Int],
                              neighbors: Array[Long],
                              keyLabels: Array[Long],
                              indices: Array[Long]) extends Serializable {
  
  lazy val size: Int = keys.length
  
  def initMsgs(model: WCCPSModel, batchSize: Int): Unit = {
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      val msgs = VFactory.sparseLongKeyLongVector(model.dim)
      for (i <- iter) {
        msgs.set(keys(i), keys(i))
        keyLabels(i) = keys(i)
      }
      model.initMsgs(msgs)
      
      println(s"part $index init ${msgs.size().toInt} msgs")
    }
  }
  
  // if label of node is larger than its neighbors',
  // change it into min among its neighbors' labels
  def process(model: WCCPSModel, batchSize: Int, numMsgs: Long, isFirstIteration: Boolean): (Int, WCCPartition) = {
    var changedNum = 0
    val inMsgs = model.readMsgs(indices)
    var batchStartTime = System.currentTimeMillis()
    
    if (numMsgs > indices.length || isFirstIteration) {
      //      val inMsgs = model.readMsgs(indices)
      for( idx <- keys.indices) {
        keyLabels(idx) = inMsgs.get(keys(idx))
      }
    }
    changedNum = makeBatchIterator(batchSize).flatMap { case(from, to) =>
      //todo can reduce memory by pulling only needed nodes from ps, time cost will increase
      //      val inMsgs = model.readMsgs(indices)
      batchStartTime = System.currentTimeMillis()
      val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())
      var changedInBatch = 0
      (from until to).foreach { idx =>
        val newLabel = minNbrLabel(idx, inMsgs)
        if (newLabel < keyLabels(idx)) {
          keyLabels(idx) = newLabel
          changedInBatch += 1
        }
        outMsgs.set(keys(idx), newLabel)
      }
      model.writeMsgs(outMsgs)
      
      println(s"part $index batch ($from, $to), cost time: ${System.currentTimeMillis() - batchStartTime} ms")
      
      Iterator(changedInBatch)
    }.sum
    (changedNum, new WCCPartition(index, keys, indptr, neighbors, keyLabels, indices))
    
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
  
  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0
    
    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, WCCPartition.this.size))
    }
    
    override def hasNext: Boolean = {
      index < WCCPartition.this.size
    }
  }
  
  def compressEdges(model: WCCPSModel): Set[(Long, Long)] = {
    val msgs = model.readMsgs(indices)
    val comEdges = collection.mutable.Set[(Long, Long)]()
    for (idx <- keys.indices) {
      var j = indptr(idx)
      val srcTag = keyLabels(idx)
      while (j < indptr(idx + 1)) {
        val nbrTag = msgs.get(neighbors(j))
        if (srcTag < nbrTag) {
          comEdges.add((srcTag, nbrTag))
        }
        j += 1
      }
    }
    comEdges.toSet
  }
  
  def edgesIfCompress(model: WCCPSModel): Long = {
    val msgs = model.readMsgs(indices)
    val comEdges = collection.mutable.Set[(Long, Long)]()
    for (idx <- keys.indices) {
      var j = indptr(idx)
      val srcTag = keyLabels(idx)
      while (j < indptr(idx + 1)) {
        val nbrTag = msgs.get(neighbors(j))
        if (srcTag < nbrTag) {
          comEdges.add((srcTag, nbrTag))
        }
        j += 1
      }
    }
    comEdges.size.toLong
  }
  
}

object WCCPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): WCCPartition = {
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
    
    new WCCPartition(index, keysArray, indptr.toIntArray(),
      neighborsArray, new Array[Long](keysArray.length), keysArray.union(neighborsArray).distinct)
  }
}
