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
package com.tencent.angel.spark.ml.graph.algo.rank.whindex

import com.tencent.angel.ml.math2.VFactory
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList


/**
  * A neighbor table tool for H-Index , Weighted H-Index and G-Index
  *
  */
class WHIndexGraphPartition(index: Int,
                            srcNodes: Array[Long],
                            indptr: Array[Int],
                            neighbors: Array[Long],
                            nodes: Array[Long],
                            neiDegrees: Array[Int],
                            WH: Array[Int],
                            weight: Array[Int]) extends Serializable {
  /**
    * init degree of each node
    *
    * @param model
    * @return
    */
  def initDegree(model: WHIndexPSModel): Int = {
    val msgs = VFactory.sparseLongKeyIntVector(model.dim)
    for (i <- srcNodes.indices) {
      msgs.set(srcNodes(i), indptr(i + 1) - indptr(i)) // srcNode's degree
    }
    model.initDegree(msgs)
    msgs.size().toInt
  }

  // calculate weight for neighbors and fill up degrees for neighbors also
  def calWeight(model: WHIndexPSModel): Int = {
    val dgs = model.readDegree(nodes)
    for (idx <- srcNodes.indices) {
      val neis = neighbors.slice(indptr(idx), indptr(idx + 1)) // neighbors
//      val dgs = model.readDegree(neis) // degree of neighbors
      for (x <- indptr(idx) until indptr(idx + 1)) {
        weight(x) = dgs.get(neighbors(x)) * neis.length
        neiDegrees(x) = dgs.get(neighbors(x))
      }
    }
    0
  }

  def updatewh(model: WHIndexPSModel): Int = {
    val msgs = VFactory.sparseLongKeyIntVector(model.dim)
    for (idx <- srcNodes.indices) {
      val dgs = neiDegrees.slice(indptr(idx), indptr(idx + 1))
      val weis = weight.slice(indptr(idx), indptr(idx + 1))
      val hArray = weis.zip(dgs).sorted
      var cnt = 0
      var i = hArray.length - 1
      while (i >= 0 && cnt < hArray(i)._1) {
        cnt += hArray(i)._2
        if (cnt > hArray(i)._1) cnt = hArray(i)._1
        i -= 1
      }
      msgs.set(srcNodes(idx), cnt)
    }
    model.updatewh(msgs)
    0
  }

  def calWH(model: WHIndexPSModel): Int = {
    val whs = model.readwh(nodes)
    for (idx <- srcNodes.indices) {
      val neis = neighbors.slice(indptr(idx), indptr(idx + 1)) // neighbors
      val w = new Array[Int](neis.length)
      neis.zipWithIndex.foreach { x =>
        w(x._2) = whs.get(x._1)
      }
//      val whs = model.readwh(neis) // wh of neighbors
      WH(idx) = w.sum
    }
    0
  }


  def save(): (Array[Long], Array[Int]) =
    (srcNodes, WH)

}

object WHIndexGraphPartition {


  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): WHIndexGraphPartition = {
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

    new WHIndexGraphPartition(index,
      srcNodesArray,
      csrPointer.toIntArray(),
      neighborsArray,
      neighborsArray.distinct,
      new Array[Int](neighborsArray.length), // neighbors' degrees
      new Array[Int](srcNodesArray.length), // srcNodes' WHs
      new Array[Int](neighborsArray.length) // edges' weights
    )
  }
}
