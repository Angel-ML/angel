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
package com.tencent.angel.spark.ml.graph.communitydetection.lpa

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, LongArrayList}

class LPAGraphPartition(index: Int,
                        keys: Array[Long],
                        indptr: Array[Int],
                        neighbors: Array[Long],
                        labels: Array[Long],
                        indices: Array[Long]) {

  def initMsgs(model: LPAPSModel): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), keys(i))
    model.initMsgs(msgs)
    msgs.size().toInt
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


  def process(model: LPAPSModel, numMsgs: Long): LPAGraphPartition = {
    val inMsgs = model.readMsgs(indices) //todo 如果ps的inMsgs没有key，会返回0
    val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())

    for (idx <- keys.indices) {
      val newLabel = calcLabel(idx, inMsgs)
      outMsgs.set(keys(idx), newLabel)
      labels(idx) = newLabel
    }

    model.writeMsgs(outMsgs)

    new LPAGraphPartition(index, keys, indptr, neighbors, labels, indices)
  }

  def calcLabel(idx: Int, inMsgs: LongLongVector): Long = {
    var j = indptr(idx)
    val labelCount = new Long2IntOpenHashMap()
    var (label, count) = (neighbors(j), 1)
    while (j < indptr(idx + 1)) {
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
      indptr.toIntArray(), neighborsArray,
      new Array[Long](keysArray.length), keysArray.union(neighborsArray).distinct)
  }
}
