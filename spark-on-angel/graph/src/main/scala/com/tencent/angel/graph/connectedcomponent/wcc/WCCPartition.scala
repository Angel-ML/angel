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

import scala.collection.mutable

class WCCPartition(index: Int,
                   keys: Array[Long],
                   indptr: Array[Int],
                   neighbors: Array[Long]) extends Serializable {
  
  def initMsgs(model: WCCPSModel, batchSize: Int): Unit = {
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      val msgs = VFactory.sparseLongKeyLongVector(iter.size)
      for (i <- iter) {
        msgs.set(keys(i), keys(i))
      }
      model.initMsgs(msgs)
      
      println(s"part $index batch (${iter.head}, ${iter.last}) init ${msgs.size().toInt} msgs")
    }
  }
  
  // if label of node is larger than its neighbors',
  // change it into min among its neighbors' labels
  def process(model: WCCPSModel, batchSize: Int): Long = {
    var changedNum = 0L
    var batchStartTime = System.currentTimeMillis()
  
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      batchStartTime = System.currentTimeMillis()
      val nbrs2pull = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
      val keys2pull = keys.slice(iter.head, iter.last + 1)
      val nodes2pull = nbrs2pull.union(keys2pull).distinct
    
      val beforePull = System.currentTimeMillis()
      val pullMsgs = model.readMsgs(nodes2pull)
      val pullMsgsTime = System.currentTimeMillis() - beforePull
    
      val outMsgs = VFactory.sparseLongKeyLongVector(iter.size)
      var changedInBatch = 0L
      iter.foreach { idx =>
        val newLabel = minNbrLabel(idx, pullMsgs)
        if (newLabel < pullMsgs.get(keys(idx))) {
          changedInBatch += 1
        }
        outMsgs.set(keys(idx), newLabel)
      }
      model.writeMsgs(outMsgs)
    
      println(s"part $index batch (${iter.head}, ${iter.last}), pull from ps cost: $pullMsgsTime ms," +
        s"total cost time: ${System.currentTimeMillis() - batchStartTime} ms")
    
      changedNum += changedInBatch
    }
    changedNum
  }
  
  // return (node, label) pairs
  def save(model: WCCPSModel, batchSize: Int): Array[(Long, Long)] = {
    keys.sliding(batchSize, batchSize).flatMap{ iter =>
      val msgs = model.readMsgs(iter)
      iter.map(k => (k, msgs.get(k)))
    }.toArray
  }
  
  def minNbrLabel(idx: Int, inMsgs: LongLongVector): Long = {
    var j = indptr(idx)
    var minLabel = inMsgs.get(keys(idx))
    while (j < indptr(idx + 1)) {
      val t = inMsgs.get(neighbors(j))
      if (minLabel > t) {
        minLabel = t
      }
      j += 1
    }
    minLabel
  }
  
  
  def compressEdges(model: WCCPSModel, batchSize: Int): Set[(Long, Long)] = {
    
    val comEdges = mutable.HashSet[(Long, Long)]()
    var batchStartTime = System.currentTimeMillis()
    
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      batchStartTime = System.currentTimeMillis()
      val nbrs2pull = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
      val keys2pull = keys.slice(iter.head, iter.last + 1)
      val nodes2pull = nbrs2pull.union(keys2pull).distinct
      
      val beforePull = System.currentTimeMillis()
      val pullMsgs = model.readMsgs(nodes2pull)
      val pullMsgsTime = System.currentTimeMillis() - beforePull
      
      iter.foreach{ idx =>
        var j = indptr(idx)
        val srcTag = pullMsgs.get(keys(idx))
        while (j < indptr(idx + 1)) {
          val nbrTag = pullMsgs.get(neighbors(j))
          if (srcTag < nbrTag) {
            comEdges.add((srcTag, nbrTag))
          }
          j += 1
        }
      }
      
      println(s"part $index, compressing edges, batch (${iter.head}, ${iter.last}), pull from ps cost: $pullMsgsTime ms," +
        s"total cost time: ${System.currentTimeMillis() - batchStartTime} ms")
    }
    comEdges.toSet
  }
  
  def edgesIfCompress(model: WCCPSModel, batchSize: Int): Long = {
    
    val comEdges = mutable.HashSet[(Long, Long)]()
    var batchStartTime = System.currentTimeMillis()
    
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      batchStartTime = System.currentTimeMillis()
      val nbrs2pull = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
      val keys2pull = keys.slice(iter.head, iter.last + 1)
      val nodes2pull = nbrs2pull.union(keys2pull).distinct
      
      val beforePull = System.currentTimeMillis()
      val pullMsgs = model.readMsgs(nodes2pull)
      val pullMsgsTime = System.currentTimeMillis() - beforePull
      
      iter.foreach{ idx =>
        var j = indptr(idx)
        val srcTag = pullMsgs.get(keys(idx))
        while (j < indptr(idx + 1)) {
          val nbrTag = pullMsgs.get(neighbors(j))
          if (srcTag < nbrTag) {
            comEdges.add((srcTag, nbrTag))
          }
          j += 1
        }
      }
      
      println(s"part $index, calculating edge num if compressed, batch (${iter.head}, ${iter.last}), pull from ps cost: $pullMsgsTime ms," +
        s"total cost time: ${System.currentTimeMillis() - batchStartTime} ms")
    }
    val size = comEdges.size.toLong
    comEdges.clear()
    size
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
    
    new WCCPartition(index, keysArray, indptr.toIntArray(), neighborsArray)
  }
}
