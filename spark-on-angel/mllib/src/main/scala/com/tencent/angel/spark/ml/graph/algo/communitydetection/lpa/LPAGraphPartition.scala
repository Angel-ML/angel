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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.lpa

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, LongArrayList}

class LPAGraphPartition(index: Int,
                        keys: Array[Long],
                        indptr: Array[Int],
                        neighbors: Array[Long],
                        indices: Array[Long]) {
  
  def initMsgs(model: LPAPSModel, batchSize: Int): Int = {
    keys.indices.sliding(batchSize, batchSize).map{ iter =>
      val msgs = VFactory.sparseLongKeyLongVector(model.dim)
      iter.foreach(idx => msgs.set(keys(idx), keys(idx)))
      model.initMsgs(msgs)
      msgs.size().toInt
    }.sum
  }
  
  def msgToString(msgs: LongLongVector): String = {
    val it = msgs.getStorage.entryIterator()
    val sb = new StringBuilder
    while (it.hasNext) {
      val entry = it.next()
      sb.append(s"${entry.getLongKey}:${entry.getLongValue} ")
    }
    sb.toString()
  }
  
  def process(model: LPAPSModel): Long = {
    val inMsgs = model.readMsgs(indices) //todo 如果ps的inMsgs没有key，会返回0
    val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())
    var changedNum = 0L
    for (idx <- keys.indices) {
      val newLabel = calcLabel(idx, inMsgs)
      if (newLabel != inMsgs.get(keys(idx))) {
        changedNum += 1
      }
      outMsgs.set(keys(idx), newLabel)
    }
    model.writeMsgs(outMsgs)
    
    changedNum
  }
  
  def process(model: LPAPSModel, batchSize: Int): Long = {
    var changedNum = 0L
    var batchCnt = 0
    keys.indices.sliding(batchSize, batchSize).foreach{ iter =>
      val before = System.currentTimeMillis()
      val nbrs2pull = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
      val keys2pull = keys.slice(iter.head, iter.last + 1)
      val nodes2pull = nbrs2pull.union(keys2pull).distinct
      
      val inMsgs = model.readMsgs(nodes2pull)
      val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())
      iter.foreach{ idx =>
        val newLabel = calcLabel(idx, inMsgs)
        if (newLabel != inMsgs.get(keys(idx))) {
          changedNum += 1
        }
        outMsgs.set(keys(idx), newLabel)
      }
      model.writeMsgs(outMsgs)
      println(s"part $index process batch $batchCnt cost: ${System.currentTimeMillis() - before} ms")
      batchCnt += 1
    }
    changedNum
  }
  
  def calcLabel(idx: Int, inMsgs: LongLongVector): Long = {
    var j = indptr(idx)
    val labelCount = new Long2IntOpenHashMap()
    var (label, count) = (inMsgs.get(neighbors(j)), 1)
    while (j < indptr(idx + 1)) {
      val nbrLabel = inMsgs.get(neighbors(j))
      labelCount.addTo(nbrLabel, 1)
      if (labelCount.get(nbrLabel) > count) {
        label = nbrLabel
        count = labelCount.get(nbrLabel)
      }
      j += 1
    }

    label
  }
  
  def save(model: LPAPSModel): Array[(Long, Long)] = {
    val inMsgs = model.readMsgs(keys)
    keys.map(key => (key, inMsgs.get(key)))
  }
}

object LPAGraphPartition {
  
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): LPAGraphPartition = {
    
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()
    
    indptr.add(0)
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      indptr.add(neighbors.size())
      keys.add(nodes)
    }
    
    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    
    new LPAGraphPartition(index, keysArray,
      indptr.toIntArray(), neighborsArray, keysArray.union(neighborsArray).distinct)
  }
  
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])], psModel: LPAPSModel): LPAGraphPartition = {
    
    val partEdgeTable = iterator.toArray
    val keys = partEdgeTable.map(_._1)
    val inMsgs = psModel.readMsgs(keys)
    
    val indptr = new IntArrayList()
    val keysBuf = new LongArrayList()
    val neighbors = new LongArrayList()
    indptr.add(0)
    
    partEdgeTable.foreach{ iter =>
      // 0 for empty label
      if (inMsgs.get(iter._1) == 0) {
        iter._2.toArray.distinct.foreach(n => neighbors.add(n))
        indptr.add(neighbors.size())
        keysBuf.add(iter._1)
      }
    }
    
    val keysArray = keysBuf.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    
    new LPAGraphPartition(index, keysArray,
      indptr.toIntArray(), neighborsArray, keysArray.union(neighborsArray).distinct)
  }
  
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])], psModel: LPAPSModel, batchSize: Int): LPAGraphPartition = {
    
    val partEdgeTable = iterator.toArray
    
    val indptr = new IntArrayList()
    val keysBuf = new LongArrayList()
    val neighbors = new LongArrayList()
    indptr.add(0)
    
    partEdgeTable.sliding(batchSize, batchSize).foreach{ iter =>
      val keysBatch = iter.map(_._1)
      val inMsgs = psModel.readMsgs(keysBatch)
      // 0 for empty label
      iter.foreach{
        case (node, ns) =>
          if (inMsgs.get(node) == 0) {
            ns.toArray.distinct.foreach(n => neighbors.add(n))
            indptr.add(neighbors.size())
            keysBuf.add(node)
          }
      }
    }
    
    val keysArray = keysBuf.toLongArray()
    val neighborsArray = neighbors.toLongArray()

    new LPAGraphPartition(index, keysArray,
      indptr.toIntArray(), neighborsArray, keysArray.union(neighborsArray).distinct)
  }
}
