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
package com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList


/**
  * A neighbor table tool for H-Index , Weighted H-Index and G-Index
  *
  */
class MultiAttrHIndexGraphPartition(index: Int,
                                    srcNodes: Array[Long],
                                    indptr: Array[Int],
                                    neighbors: Array[Long],
                                    srcNodesHIndex: Array[Array[Int]],
                                    nodes: Array[Long]) extends Serializable {
  /**
    * init degree of each node
    *
    * @param model
    * @return
    */

  def process(model: MultiAttrHIndexPSModel, batchSize: Int): (Array[Long], Array[Array[Int]]) = {
    srcNodes.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val beforePullTs = System.currentTimeMillis()
      val pullNodes = neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last + 1)).distinct
      val a = model.readAttributes(pullNodes)
      println(s"partition $index: process, " +
        s"pull ${pullNodes.length} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      var num1 = 0
      var num2 = 0
      //    val attrs = model.readAttributes(nodes)
      val beforeComputeTs = System.currentTimeMillis()
      for (idx <- nodesIndex) {
        val numNeighbors = indptr(idx + 1) - indptr(idx)
        val attrsForHIndex = new Array[Array[Float]](numNeighbors)
        for (i <- 0 until numNeighbors) {
          val q = a.get(neighbors(indptr(idx) + i))
          if (q == null) num1 += 1
          attrsForHIndex(i) = q
        }
        srcNodesHIndex(idx) = calc(attrsForHIndex)
        if (srcNodesHIndex(idx) == null) num2 += 1
      }
      println(s"partition $index: process, " +
        s"compute ${nodesIndex.length} nodes, " +
        s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")
    }
//    (num1, num2)
    (srcNodes, srcNodesHIndex)
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

  def save(): (Array[Long], Array[Array[Int]]) =
    (srcNodes, srcNodesHIndex)

  def read(model: MultiAttrHIndexPSModel): Long = {
    val x = model.readAttributes(srcNodes)
    val y = x.values().toArray().filter(x=>x!=null)
    y.length
  }

}

object MultiAttrHIndexGraphPartition {


  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): MultiAttrHIndexGraphPartition = {
    val csrPointer = new IntArrayList()
    csrPointer.add(0)

    val srcNodes = new LongArrayList()
    val neighbors = new LongArrayList()

    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val (node, ns) = iterator.next()
      srcNodes.add(node)
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      csrPointer.add(neighbors.size())
      maxDegree = math.max(ns.size, maxDegree)
    }

    val srcNodesArray = srcNodes.toLongArray()
    val neighborsArray = neighbors.toLongArray()

    new MultiAttrHIndexGraphPartition(index,
      srcNodesArray,
      csrPointer.toIntArray(),
      neighborsArray,
      new Array[Array[Int]](srcNodesArray.length),
      neighborsArray.union(srcNodesArray).distinct
    )
  }
}
