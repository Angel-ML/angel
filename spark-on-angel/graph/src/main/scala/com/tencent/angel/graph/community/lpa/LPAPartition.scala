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
package com.tencent.angel.graph.community.lpa

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, LongArrayList}

class LPAPartition(index: Int,
                   keys: Array[Long],
                   idxptr: Array[Int],
                   neighbors: Array[Long],
                   labels: Array[Long],
                   indices: Array[Long]) {

  def initMsgs(model: LPAPSModel): Long = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices) {
      msgs.set(keys(i), keys(i))
    }
    model.initMsgs(msgs)
    msgs.size()
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


  def process(model: LPAPSModel): (LPAPartition, Long) = {
    val inMsgs = model.readMsgs(indices) //todo 如果ps的inMsgs没有key，会返回0
    val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())
    var numChanged = 0
    for (idx <- keys.indices) {
      val newLabel = calcLabel(idx, inMsgs)
      if (inMsgs.get(keys(idx)) != newLabel) {
        numChanged += 1
      }
      outMsgs.set(keys(idx), newLabel)
      labels(idx) = newLabel
    }

    model.writeMsgs(outMsgs)

    (new LPAPartition(index, keys, idxptr, neighbors, labels, indices), numChanged)
  }

  def calcLabel(idx: Int, inMsgs: LongLongVector): Long = {
    var j = idxptr(idx)
    val labelCount = new Long2IntOpenHashMap()
    var (label, count) = (inMsgs.get(neighbors(j)), 1)
    while (j < idxptr(idx + 1)) {
      labelCount.addTo(inMsgs.get(neighbors(j)), 1)
      if (labelCount.get(inMsgs.get(neighbors(j))) > count) {
        label = inMsgs.get(neighbors(j))
        count = labelCount.get(inMsgs.get(neighbors(j)))
      }
      j += 1
    }

    label
  }

  def save(): (Array[Long], Array[Long]) =
    (keys, labels)
}

object LPAPartition {

  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): (LPAPartition, Long) = {

    val idxptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()

    idxptr.add(0)
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      idxptr.add(neighbors.size())
      keys.add(nodes)
    }

    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val nodes = keysArray.union(neighborsArray).distinct

    val graphPartition = new LPAPartition(index, keysArray, idxptr.toIntArray(),
      neighborsArray, new Array[Long](keysArray.length), nodes)
    (graphPartition, nodes.length)
  }
}
