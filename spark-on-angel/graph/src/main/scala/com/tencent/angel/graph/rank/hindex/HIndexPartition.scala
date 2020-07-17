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
package com.tencent.angel.graph.rank.hindex

import java.util.{Arrays => JArrays}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

/**
  * A neighbor table tool for H-Index , Weighted H-Index and G-Index
  *
  * @param index          EdgePartition's partition id
  * @param srcNodes       src nodes
  * @param indptr         csr pointer index
  * @param neighbors      each src node's neighbors
  * @param srcNodesHIndex store each src node's h-index value, w-index and g-index
  * @param neiCores       neighbor core
  * @param indices        all node in this partition
  * @param hIndex
  */
class HIndexPartition(index: Int,
                      srcNodes: Array[Long],
                      indptr: Array[Int],
                      neighbors: Array[Long],
                      srcNodesHIndex: Array[(Int, Int, Int)],
                      neiCores: Array[Int],
                      indices: Array[Long],
                      hIndex: Array[Int]) extends Serializable {
  /**
    * init degree of each node
    *
    * @param model
    * @return
    */
  def initMsgs(model: HIndexPSModel): Int = {
    val msgs = VFactory.sparseLongKeyIntVector(model.dim)
    for (i <- srcNodes.indices) {
      msgs.set(srcNodes(i), indptr(i + 1) - indptr(i)) // srcNode's degree
    }
    model.initMsgs(msgs)
    msgs.size().toInt
  }

  /**
    *
    * @param model
    * @return
    */
  def process(model: HIndexPSModel): (Array[Long], Array[(Int, Int, Int)]) = {
    val inMsgs = model.readMsgs(indices) // read all nodes of this subGraph,include srcIds and dstIds
    var sum = 0
    for (idx <- srcNodes.indices) { //handle each srcNode
      val newIndex = calcOneFirst(idx, inMsgs)
      sum += newIndex._1
      srcNodesHIndex(idx) = newIndex
    }
    (srcNodes, srcNodesHIndex)
  }


  /**
    * compute h-index of idx
    *
    * @param idx
    * @param inMsgs
    * @return
    */
  def calcOneFirst(idx: Int, inMsgs: LongIntVector): (Int, Int, Int) = {
    srcNodesHIndex(idx) = (inMsgs.get(srcNodes(idx)), 0, 0) // init hindex with degree
    var j = indptr(idx)
    while (j < indptr(idx + 1)) {
      neiCores(j) = inMsgs.get(neighbors(j)) //init  neighbor's core with it's degree
      j += 1
    }
    System.arraycopy(neiCores, indptr(idx), hIndex, 0, indptr(idx + 1) - indptr(idx))
    val start = 0
    val end = indptr(idx + 1) - indptr(idx)
    JArrays.sort(hIndex, 0, end) //sort

    (calcHIndex(start, end), calcGIndex(start, end), calcWIndex(start, end))
  }

  /**
    * compute h-index
    *
    * @param start the start index of a node's neighbors
    * @param end   the end index of a node's neighbors
    * @return h-index value
    */
  def calcHIndex(start: Int, end: Int): Int = {
    var i = end - 1
    var cnt = 1
    while (i >= start && hIndex(i) >= cnt) {
      cnt += 1
      i -= 1
    }

    cnt - 1
  }

  /**
    * compute g-index
    *
    * @param start the start index of a node's neighbors
    * @param end   the end index of a node's neighbors
    * @return g-index value
    */
  def calcGIndex(start: Int, end: Int): Int = {
    var i = end - 1
    var cnt = 0
    var g = 0
    var gIndex = 0
    while (i >= start && cnt >= g) {
      gIndex += 1
      cnt += hIndex(i)
      g = (end - i) * (end - i)
      i -= 1
    }
    if (cnt >= g) {
      gIndex
    } else {
      gIndex - 1
    }
  }

  /**
    * compute w-index
    *
    * @param start the start index of a node's neighbors
    * @param end   the end index of a node's neighbors
    * @return w-index value
    */
  def calcWIndex(start: Int, end: Int): Int = {
    var i = end - 1
    var cnt = 1
    var j = 1
    while (i >= start && hIndex(i) >= 10 * cnt) {
      if (j >= cnt) {
        j += 1
        cnt += 1
      }
      i -= 1
    }
    cnt - 1
  }

  /**
    * save the  h-index, g-index and w-index of node
    *
    * @return nodes and the index result
    */
  def save(): (Array[Long], Array[(Int, Int, Int)]) =
    (srcNodes, srcNodesHIndex)

}

object HIndexPartition {


  /**
    * graph partition, store  node and it's neighbors in CSR format
    *
    * @param index    partition id
    * @param iterator node and it's neighbors
    * @return graph partition
    */
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): HIndexPartition = {
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

    new HIndexPartition(index, // EdgePartition partition id
      srcNodesArray, // all src nodes
      csrPointer.toIntArray(), // csr pointer
      neighborsArray, // all src node's neighbors
      new Array[(Int, Int, Int)](srcNodesArray.length), //src node's h-index,w-index,and g-index
      new Array[Int](neighborsArray.length), // neighbors length
      srcNodesArray.union(neighborsArray).distinct, // all nodes include srcNodes and dstNodes
      new Array[Int](maxDegree)
    )
  }
}
