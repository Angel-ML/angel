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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.hanp

import java.util.{Arrays => JArrays}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongIntVector, LongLongVector}
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class HANPGraphPartition(index: Int,
                         preserveRate: Float,
                         delta: Float,
                          keys: Array[Long],
                          indptr: Array[Int],
                          neighbors: Array[Long],
                          keyLabels: Array[(Long, Float)],
                          indices: Array[Long],
                          weights: Array[Float]) extends Serializable {

  def initDegrees(model: HANPPSModel): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), indptr(i + 1) - indptr(i))
    model.initDegree(msgs)
    msgs.size().toInt
  }

  def initScores(model: HANPPSModel): Int = {
    val msgs = VFactory.sparseLongKeyFloatVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), 1.0f)
    model.initScore(msgs)
    msgs.size().toInt
  }

  def initLabels(model: HANPPSModel): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), keys(i))
    model.initLabel(msgs)
    msgs.size().toInt
  }

  def process(model: HANPPSModel): HANPGraphPartition = {
    // pull msgs
    val beforePullTs = System.currentTimeMillis()
    val scoreInMsgs = model.readScore(indices)
    val degrees = model.readDegree(indices)
    val labels = model.readLabel(indices)
    println(s"partition $index: process, " +
      s"pull ${indices.length} nodes from ps, " +
      s"cost ${System.currentTimeMillis() - beforePullTs} ms")

    // calculate
    val beforeComputeTs = System.currentTimeMillis()
    val labelOutMsgs = VFactory.sparseLongKeyLongVector(keys.length)
    val scoreOutMsgs = VFactory.sparseLongKeyFloatVector(keys.length)
    for (idx <- keys.indices) {
      val (newLabel, newScore) = if (Random.nextDouble() < preserveRate) {
        keyLabels(idx)
      } else {
        calc(idx, scoreInMsgs, degrees, labels)
      }
      labelOutMsgs.set(keys(idx), newLabel)
      scoreOutMsgs.set(keys(idx), newScore)
      keyLabels(idx) = (newLabel, newScore)
    }
    println(s"partition $index: process, " +
      s"compute ${keys.length} nodes, " +
      s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")

    val beforePushTs = System.currentTimeMillis()
    model.writeLabel(labelOutMsgs)
    model.writeScore(scoreOutMsgs)
    println(s"partition $index: process, " +
      s"push ${keys.length} nodes to ps, " +
      s"cost ${System.currentTimeMillis() - beforePushTs} ms")


    new HANPGraphPartition(index, preserveRate, delta, keys, indptr,
        neighbors, keyLabels, indices, weights)
    }

  def calc(idx: Int, scoreInMsgs: LongFloatVector, degrees: LongLongVector, labelInMsgs: LongLongVector): (Long, Float) = {
    var j = indptr(idx)
    // label -> (labelToChoose, score) labelToChoose = neighborScore * neighborDegree * edgeWeight
    // to do: all neighbors' score is less than or equal to 0
    val temp = mutable.Map(-1l -> (0f, 0f))
    while (j < indptr(idx + 1)) {
      val l = labelInMsgs.get(neighbors(j))
      val s = math.max(scoreInMsgs.get(neighbors(j)), 0.0f)
//      val s = if (scoreInMsgs.get(neighbors(j)) > 0) scoreInMsgs.get(neighbors(j)) else 0.0f
      val d = degrees.get(neighbors(j))
      val w = weights(j)
      val labelToChoose = s * d * w
      val (inLabelToChoose, inScore) = temp.getOrElse(l, (0.0f, 0.0f))
      temp += ((l, (labelToChoose + inLabelToChoose, math.max(inScore, s))))
      j += 1
    }
    temp.remove(-1)
    assert(temp.nonEmpty, s"mutable map for key ${keys(idx)} is empty.")
    val newLabel = temp.maxBy(_._2._1)
    if (newLabel._1 == keyLabels(idx)._1) {
      (newLabel._1, newLabel._2._1)
    } else {
      (newLabel._1, newLabel._2._1 - delta)
    }
  }

  def save(): (Array[Long], Array[Long]) =
    (keys, keyLabels.map(_._1))
}


object HANPGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])], delta: Float, preserveRate: Float): HANPGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()
    val weights = new FloatArrayList()

    indptr.add(0)
    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2)
      ns.toArray.distinct.sorted.foreach { case (n, w) =>
        neighbours.add(n)
        weights.add(w)
      }
      indptr.add(neighbours.size())
      keys.add(node)
      maxDegree = math.max(ns.size, maxDegree)
    }

    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()
    val weightsArray = weights.toFloatArray()

    new HANPGraphPartition(index,
      preserveRate,
      delta,
      keysArray,
      indptr.toIntArray(),
      neighboursArray,
      keysArray.map(x=>(x, 1f)), // initial key label and score
      keysArray.union(neighboursArray).distinct,
      weightsArray)
  }

}
