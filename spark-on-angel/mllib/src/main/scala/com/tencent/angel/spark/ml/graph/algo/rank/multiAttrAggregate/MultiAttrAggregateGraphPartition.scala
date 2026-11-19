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
package com.tencent.angel.spark.ml.graph.algo.rank.multiAttrAggregate

import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex.MultiAttrHIndexPSModel
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList

import scala.collection.mutable.ArrayBuffer


/**
  * A neighbor table tool for node attrs aggregation , sum/mean/max/min
  *
  */
class MultiAttrAggregateGraphPartition(index: Int,
                                    srcNodes: Array[Long],
                                    indptr: Array[Int],
                                    neighbors: Array[Long],
                                    weights: Array[Float],
                                    nodes: Array[Long]) extends Serializable {

  // aggregate node attrs, sum/mean/max/min
  def process(model: MultiAttrHIndexPSModel, batchSize: Int, numFeatures: Int): Array[(Long, Array[Float])] = {
    val result = ArrayBuffer[(Long, Array[Float])]()
    srcNodes.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val beforePullTs = System.currentTimeMillis()
      val pullNodes = neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last + 1)).distinct
      val a = model.readAttributes(pullNodes)
      println(s"partition $index: process, " + s"pull ${pullNodes.length} nodes from ps, " + s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val beforeComputeTs = System.currentTimeMillis()
      for (idx <- nodesIndex) {
        var nbrIndex = indptr(idx)
        var tagIndex = 0
        val aggre = new Array[Float](numFeatures * 4)
        var numValidNbrs = 0
        while (nbrIndex < indptr(idx + 1)) {
          val next = a.get(neighbors(nbrIndex))
          if (next != null && next.length == numFeatures) {
            numValidNbrs += 1
            val w = weights(nbrIndex)
            for (i <- 0 until numFeatures) {
              val t = next(i) * w
              aggre(i * 4) = t
              aggre(i * 4 + 1) = t
              aggre(i * 4 + 2) = t
              aggre(i * 4 + 3) = t
            }
            tagIndex = nbrIndex
            nbrIndex = indptr(idx + 1) // finish the loop
          } else {
            tagIndex = nbrIndex
            nbrIndex += 1
          }
        }
        nbrIndex = tagIndex + 1
        while (nbrIndex < indptr(idx + 1)) {
          val w = weights(nbrIndex)
          val next = a.get(neighbors(nbrIndex))
          if (next != null && next.length == numFeatures) {
            numValidNbrs += 1
            for (j <- 0 until numFeatures) {
              val value = next(j) * w
              aggre(j * 4) = aggre(j * 4) + value
              aggre(j * 4 + 2) = math.max(aggre(j * 4 + 2), value)
              aggre(j * 4 + 3) = math.min(aggre(j * 4 + 3), value)
            }
          }
          nbrIndex += 1
        }
        if (numValidNbrs > 0) {
          for (i <- 0 until numFeatures) {
            aggre(i * 4 + 1) = aggre(i * 4) / numValidNbrs
          }
          result.append((srcNodes(idx), aggre))
        }
      }
      println(s"partition $index: process, " + s"compute ${nodesIndex.length} nodes, " + s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")
    }
    result.toArray
  }

  def process1(model: MultiAttrHIndexPSModel, batchSize: Int, numFeatures: Int): Array[(Long, Array[Float])] = {
    val result = ArrayBuffer[(Long, Array[Float])]()
    srcNodes.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val beforePullTs = System.currentTimeMillis()
      val pullNodes = neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last + 1)).distinct
      val a = model.readAttributes(pullNodes)
      println(s"partition $index: process, " + s"pull ${pullNodes.length} nodes from ps, " + s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val beforeComputeTs = System.currentTimeMillis()
      for (idx <- nodesIndex) {
        var nbrIndex = indptr(idx)
        var tagIndex = 0
        val aggre = new Array[Float](numFeatures)
        var numValidNbrs = 0
        while (nbrIndex < indptr(idx + 1)) {
          val next = a.get(neighbors(nbrIndex))
          if (next != null && next.length == numFeatures) {
            numValidNbrs += 1
            val w = weights(nbrIndex)
            for (i <- 0 until numFeatures) {
              val t = next(i) * w
              aggre(i) = t
            }
            tagIndex = nbrIndex
            nbrIndex = indptr(idx + 1) // finish the loop
          } else {
            tagIndex = nbrIndex
            nbrIndex += 1
          }
        }
        nbrIndex = tagIndex + 1
        while (nbrIndex < indptr(idx + 1)) {
          val w = weights(nbrIndex)
          val next = a.get(neighbors(nbrIndex))
          if (next != null && next.length == numFeatures) {
            numValidNbrs += 1
            for (j <- 0 until numFeatures) {
              val value = next(j) * w
              aggre(j) = j % 4 match {
                case 0 => aggre(j) + value
                case 1 => aggre(j) + value
                case 2 => math.max(aggre(j), value)
                case 3 => math.min(aggre(j), value)
              }
            }
          }
          nbrIndex += 1
        }
        if (numValidNbrs > 0) {
          for (i <- 0 until numFeatures) {
            if (i % 4 == 1) aggre(i) = aggre(i) / numValidNbrs
          }
          result.append((srcNodes(idx), aggre))
        }
      }
      println(s"partition $index: process, " + s"compute ${nodesIndex.length} nodes, " + s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")
    }
    result.toArray
  }

  /**
    * aggregate node attributes
    *
    * @param nbrAttrs the attributes of node's neighbors
    * @return
    */
  def aggregateFunc(nbrAttrs: Array[Array[Float]], nbrWeights: Array[Float]): Array[Float] = {
    val num = nbrAttrs.head.length
    val nbrsNum = nbrAttrs.length
    val res = new Array[Float](num * 4)
    val attr = new Array[Float](nbrsNum)
    for (i <- 0 until num) {
      for (j <- 0 until nbrsNum) {
        attr(j) = nbrAttrs(j)(i)
      }
      val weightedAttr = attr.zip(nbrWeights).map{ case (a, f) => a * f}
      val sum = weightedAttr.sum
      val mean = sum / weightedAttr.length
      val max = weightedAttr.max
      val min = weightedAttr.min
//      val hindex = calcHIndex(atrr.sorted)
      res(i * 4) = sum
      res(i * 4 + 1) = mean
      res(i * 4 + 2) = max
      res(i * 4 + 3) = min
    }
    res
  }

  def aggregateFunc1(nbrAttrs: Array[Array[Float]], nbrWeights: Array[Float]): Array[Float] = {
    val num = nbrAttrs.head.length
    val nbrsNum = nbrAttrs.length
    val res = new Array[Float](num)
    val attr = new Array[Float](nbrsNum)
    for (i <- 0 until num) {
      for (j <- 0 until nbrsNum) {
        attr(j) = nbrAttrs(j)(i)
      }
      val weightedAttr = attr.zip(nbrWeights).map{ case (a, f) => a * f}
      i % 4 match {
        case 0 => res(i) = weightedAttr.sum
        case 1 => res(i) = weightedAttr.sum / weightedAttr.length
        case 2 => res(i) = weightedAttr.max
        case 3 => res(i) = weightedAttr.min
      }
    }
    res
  }


  def calc(attrs: Array[Array[Float]]): Array[Int] = {
    val numAttrs = attrs(0).length
    val numNeighbors = attrs.length
    val indexResult = new Array[Int](numAttrs)
    for (a <- 0 until numAttrs) {
      val arr = new Array[Float](numNeighbors)
      for (n <- 0 until numNeighbors) {
        arr(n) = attrs(n)(a)
      }
      indexResult(a) = calcHIndex(arr.sorted)
    }
    indexResult
  }

  def calcHIndex(array: Array[Float]): Int = {
    var i = array.length - 1
    var cnt = 1
    //compute h-index
    while (i >= 0 && array(i) >= cnt) {
      cnt += 1
      i -= 1
    }

    cnt - 1
  }

  def calcGIndex(array: Array[Int]): Int = {
    val start = 0
    val end = array.length
    var i = end - 1
    var cnt = 0
    var g2 = 0
    var gIndex = 0
    while (i >= start && cnt >= g2) {
      gIndex += 1
      cnt += array(i)
      g2 = (end - i) * (end - i)
      i -= 1
    }
    if (cnt >= g2) {
      gIndex
    } else {
      gIndex - 1
    }
  }

  def calcWIndex(array: Array[Int]): Int = {
    val start = 0
    val end = array.length
    var i = end - 1
    var cnt = 1
    var j = 1
    while (i >= start && array(i) >= 10 * cnt) {
      if (j >= cnt) {
        j += 1
        cnt += 1
      }
      i -= 1
    }
    cnt - 1
  }


  def read(model: MultiAttrHIndexPSModel): Long = {
    val x = model.readAttributes(srcNodes)
    val y = x.values().toArray().filter(x=>x!=null)
    y.length
  }

}

object MultiAttrAggregateGraphPartition {


  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])]): MultiAttrAggregateGraphPartition = {
    val csrPointer = new IntArrayList()
    csrPointer.add(0)

    val srcNodes = new LongArrayList()
    val neighbors = new LongArrayList()
    val weights = new FloatArrayList()

    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val (node, ns) = iterator.next()
      srcNodes.add(node)
      ns.toArray.distinct.foreach { case (n, f) =>
        neighbors.add(n)
        weights.add(f)
      }
      csrPointer.add(neighbors.size())
      maxDegree = math.max(ns.size, maxDegree)
    }

    val srcNodesArray = srcNodes.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val weightsArray = weights.toFloatArray()

    println(s"apply, part $index, numKeys=${srcNodesArray.length}, numEdges=${neighborsArray.length}")
    new MultiAttrAggregateGraphPartition(index,
      srcNodesArray,
      csrPointer.toIntArray(),
      neighborsArray,
      weightsArray,
      neighborsArray.union(srcNodesArray).distinct
    )
  }
}
