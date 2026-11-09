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
package com.tencent.angel.spark.ml.graph.algo.feature.neighborVectorDistance

import com.tencent.angel.spark.ml.ann.DistanceFunctions
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import scala.collection.mutable.ArrayBuffer

class neighborVectorDistanceGraphPartition(index: Int,
                                           srcNodesArray: Array[Long],
                                           indptr: Array[Int],
                                           neighbors: Array[Long],
                                           batchSize: Int) {


  def process(model: neighborVectorDistancePSModel, disFunc: String): Array[(Long, Long, Float)] = {
    val distanceFunction = DistanceFunctions.str2DistanceFunction(disFunc)
    val savedPart = new ArrayBuffer[(Long, Long, Float)]()
    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val pullNodes = (neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last + 1)) ++ srcNodesArray.slice(nodesIndex.head, nodesIndex.last + 1)).distinct
      val comInMsgs = model.readQueries(pullNodes)

      //computer each node
      for (idx <- nodesIndex) {
        val srcNode = srcNodesArray(idx)
        val srcNodeVec = comInMsgs.getOrDefault(srcNode, null)
        if (srcNodeVec != null) {
          var j = indptr(idx)
          while (j < indptr(idx + 1)) {
            val nei = neighbors(j)
            val neiNodeVec = comInMsgs.getOrDefault(nei, null)
            if (neiNodeVec != null) {
              val distance = distanceFunction.distance(srcNodeVec.getData, neiNodeVec.getData)
              savedPart.append((srcNode, nei, distance))
            }
            j += 1
          }
        }
      }
    }
    savedPart.toArray
  }
}

object neighborVectorDistanceGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])], batchSize: Int): neighborVectorDistanceGraphPartition = {
    val indptr = new IntArrayList()
    val srcNodes = new LongArrayList()
    val neighbors = new LongArrayList()

    indptr.add(0)
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()
      ns.toArray.distinct.sorted.foreach { n => neighbors.add(n) }
      indptr.add(neighbors.size())
      srcNodes.add(nodes)
    }
    val srcNodesArray = srcNodes.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    new neighborVectorDistanceGraphPartition(index, srcNodesArray, indptr.toIntArray(), neighborsArray, batchSize)
  }
}
