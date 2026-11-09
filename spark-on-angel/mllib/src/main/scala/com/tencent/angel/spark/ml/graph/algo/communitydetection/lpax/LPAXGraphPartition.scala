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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.lpax

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{LongArrayList}

class LPAXGraphPartition(index: Int,
                        keys: Array[Long],
                        keyFlags: Array[Int],
                        indptr: Array[Int],
                        neighbors: Array[Long],
                        weights: Array[Float],
                        labels0: Array[Float],
                        labels1: Array[Float],
                        indices: Array[Long]) {

  def initMsgs(model0: LPAXPSModel, model1: LPAXPSModel): Int = {
    val msgs0 = VFactory.sparseLongKeyFloatVector(model0.dim)
    val msgs1 = VFactory.sparseLongKeyFloatVector(model1.dim)
    for (i <- keys.indices){
        msgs0.set(keys(i), 1.0f - keyFlags(i))
        msgs1.set(keys(i), keyFlags(i).toFloat)
    }
    model0.initMsgs(msgs0)
    model1.initMsgs(msgs1)
    msgs0.size().toInt
  }

  def msgToString(msgs: LongFloatVector): String = {
    val it = msgs.getStorage.entryIterator()
    val sb = new StringBuilder
    while (it.hasNext) {
      val entry = it.next()
      sb.append(s"${entry.getLongKey}:${entry.getFloatValue} ")
    }
    sb.toString()
  }

  def process(model0: LPAXPSModel, model1: LPAXPSModel): LPAXGraphPartition = {
    val inMsgs0 = model0.readMsgs(indices) //todo 如果ps的inMsgs没有key，会返回0
    val outMsgs0 = VFactory.sparseLongKeyFloatVector(inMsgs0.dim())
    val inMsgs1 = model1.readMsgs(indices) //todo 如果ps的inMsgs没有key，会返回0
    val outMsgs1 = VFactory.sparseLongKeyFloatVector(inMsgs1.dim())
    
    for (idx <- keys.indices) {
      val (newLabel0, newLabel1) = calcLabel(idx, inMsgs0, inMsgs1)
      outMsgs0.set(keys(idx), newLabel0)
      outMsgs1.set(keys(idx), newLabel1)
      labels0(idx) = newLabel0
      labels1(idx) = newLabel1
    }

    model0.writeMsgs(outMsgs0)
    model1.writeMsgs(outMsgs1)

    new LPAXGraphPartition(index, keys, keyFlags, indptr, neighbors, weights, labels0, labels1, indices)
  }

  def calcLabel(idx: Int, inMsgs0: LongFloatVector, inMsgs1: LongFloatVector): (Float, Float) = {
    if (keyFlags(idx) == 1) {
      (0, 1)
    }
    else {
      var j = indptr(idx)
      var sumW0 = inMsgs0.get(keys(idx))
      var sumW1 = inMsgs1.get(keys(idx))
 
      while (j < indptr(idx + 1)) {
        val nbrAcp0 = inMsgs0.get(neighbors(j)) * weights(j)
        val nbrAcp1 = inMsgs1.get(neighbors(j)) * weights(j)
        
        sumW0 = sumW0 + nbrAcp0
        sumW1 = sumW1 + nbrAcp1
        j += 1
      }

      val sumW = sumW0 + sumW1
      (sumW0 / sumW, sumW1 / sumW)
    }
  }

  def save(): (Array[Long], Array[Float], Array[Float]) =
    (keys, labels0, labels1)
}

object LPAXGraphPartition {

  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float, Int)])]): LPAXGraphPartition = {

    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val keyFlags = new IntArrayList() 
    val neighbors = new LongArrayList()

    // in-weights. not necessary in-weight == out-weight
    // in-weights needed. so the graph was organized in (dst, src)
    val weights = new FloatArrayList()

    indptr.add(0)
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()

      ns.toArray.distinct.foreach(n => {
          neighbors.add(n._1)
          weights.add(n._2)
          })
      indptr.add(neighbors.size())
      keys.add(nodes)
      keyFlags.add(ns.head._3)
    }

    val keysArray = keys.toLongArray()
    val keyFlagsArray = keyFlags.toIntArray()
    val neighborsArray = neighbors.toLongArray()
    val weightsArray = weights.toFloatArray()

    new LPAXGraphPartition(index, keysArray, keyFlagsArray, 
      indptr.toIntArray(), neighborsArray, weightsArray,
      new Array[Float](keysArray.length), new Array[Float](keysArray.length),
      keysArray.union(neighborsArray).distinct)
  }
}
