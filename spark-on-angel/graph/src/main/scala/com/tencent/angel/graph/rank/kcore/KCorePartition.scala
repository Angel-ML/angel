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

package com.tencent.angel.graph.rank.kcore

import java.util.{Arrays => JArrays}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

/**
  * KCoreGraphPartition implementation
  *
  * @param index     partition index
  * @param keys      node ids in this partition
  * @param indptr    node neighbor index, the ith node neighbors index range is [indptr(i), indptr(i+1))
  * @param neighbors node neighbors
  * @param keyCores  node core
  * @param neiCores  neighbor core
  * @param indices   all node in this partition
  * @param hIndices  hIndices
  */
private[kcore]
class KCorePartition(index: Int,
                     keys: Array[Long],
                     idxptr: Array[Int],
                     neighbors: Array[Long],
                     keyCores: Array[Int],
                     neiCores: Array[Int],
                     indices: Array[Long],
                     hIndices: Array[Int]) extends Serializable {
  /**
    * use the degree to init vertices core-value
    *
    * @param model
    * @return
    */
  def initMsgs(model: KCorePSModel): Int = {
    val msgs = VFactory.sparseLongKeyIntVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), idxptr(i + 1) - idxptr(i))
    model.initMsgs(msgs)
    msgs.size().toInt
  }

  def process(model: KCorePSModel, numMsgs: Long, isFirstIteration: Boolean): KCorePartition = {

    val inMsgs = if (numMsgs > indices.length || isFirstIteration) model.readMsgs(indices) else model.readAllMsgs()
    val outMsgs = VFactory.sparseLongKeyIntVector(inMsgs.dim())
    for (idx <- keys.indices) {
      val newIndex = if (isFirstIteration) calcOneFirst(idx, inMsgs) else calcOne(idx, inMsgs)
      if (newIndex < keyCores(idx)) {
        outMsgs.set(keys(idx), newIndex)
        keyCores(idx) = newIndex
      }
    }

    model.writeMsgs(outMsgs)
    new KCorePartition(index, keys, idxptr, neighbors, keyCores, neiCores, indices, hIndices)

  }

  def calcOne(idx: Int, inMsgs: LongIntVector): Int = {
    var j = idxptr(idx)
    var flag = false
    while (j < idxptr(idx + 1)) {
      val t = inMsgs.get(neighbors(j))
      if (t != 0 && t != neiCores(j)) {
        neiCores(j) = t
        flag = true
      }
      j += 1
    }

    if (flag)
      calcHIndex(neiCores, idxptr(idx), idxptr(idx + 1))
    else
      keyCores(idx)
  }

  def calcOneFirst(idx: Int, inMsgs: LongIntVector): Int = {
    keyCores(idx) = inMsgs.get(keys(idx))
    var j = idxptr(idx)
    while (j < idxptr(idx + 1)) {
      neiCores(j) = inMsgs.get(neighbors(j))
      j += 1
    }
    calcHIndex(neiCores, idxptr(idx), idxptr(idx + 1))
  }

  def calcHIndex(citations: Array[Int], from: Int, to: Int): Int = {
    System.arraycopy(citations, from, hIndices, 0, to - from)
    val start = 0
    val end = to - from
    JArrays.sort(hIndices, 0, end)
    var i = end - 1
    var cnt = 1
    while (i >= start && hIndices(i) >= cnt) {
      cnt += 1
      i -= 1
    }
    cnt - 1
  }

  def save(): (Array[Long], Array[Int]) =
    (keys, keyCores)
}


private[kcore]
object KCorePartition {
  /**
    * user CSR (index pointer) store the adjacency table of vertex
    *
    * @param index
    * @param iterator Adjacency table of vertex
    * @return
    */
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): KCorePartition = {
    //csr index pointer
    val idxptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()

    idxptr.add(0)
    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2.toArray.distinct)
      ns.foreach(n => neighbours.add(n))
      idxptr.add(neighbours.size())
      keys.add(node)
      maxDegree = math.max(ns.size, maxDegree)
    }

    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()

    new KCorePartition(index, keysArray, idxptr.toIntArray(),
      neighboursArray, new Array[Int](keysArray.length),
      new Array[Int](neighboursArray.length),
      keysArray.union(neighboursArray).distinct,
      new Array[Int](maxDegree))
  }

  def apply(index: Int, keys: Array[Long],
            idxptr: Array[Int],
            neighbors: Array[Long],
            keyCores: Array[Int],
            neiCores: Array[Int],
            indices: Array[Long],
            hIndices: Array[Int]): KCorePartition = {
    new KCorePartition(index, keys, idxptr,
      neighbors, keyCores, neiCores, indices, hIndices)
  }

}
