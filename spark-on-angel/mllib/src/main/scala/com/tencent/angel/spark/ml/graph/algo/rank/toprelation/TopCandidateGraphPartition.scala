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
package com.tencent.angel.spark.ml.graph.algo.rank.toprelation
import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex.MultiAttrHIndexPSModel
import com.tencent.angel.spark.ml.graph.utils.collection.OpenHashMap
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

class TopCandidateGraphPartition(index: Int,
                                 srcNodes: Array[Long],
                                 indptr: Array[Int],
                                 neighbors: Array[Long],
                                 srcNodesTopkFreq: Array[(String, String, String, String)],
                                 nodes: Array[Long],
                                 weights: Array[Float]) extends Serializable {
  /**
    * init degree of each node
    *
    * @param model
    * @return
    */

  def process(model: MultiAttrHIndexPSModel, batchSize: Int, k: Int): (Array[Long], Array[(String, String, String, String)]) = {
    srcNodes.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val beforePullTs = System.currentTimeMillis()
      val pullNodes = neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last + 1)).distinct
      val a = model.readAttributes(pullNodes)
      println(s"partition $index: process, " +
        s"pull ${pullNodes.length} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val beforeComputeTs = System.currentTimeMillis()
      for (idx <- nodesIndex) {
        // requested songs from friends
        // requested songs from close friends
        // most requested songs from friends
        // most requested songs from close friends
        val map = new OpenHashMap[Long, (Float, Float, Float, Float)]()
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          val weight = weights(j)
          val attrs = a.get(neighbors(j))
          var n = 0
          while (n < attrs.length) {
            val songId = attrs(n).toLong
            val count = attrs(n + 1)
            val oldCount = if (map.contains(songId)) map(songId) else (0f, 0f, 0f, 0f)
            //            map.update(songId, oldCount + Array(1, 1 * weight, count, count * weight))
            map.update(songId, (oldCount._1 + 1f, oldCount._2 + 1f * weight, oldCount._3 + count, oldCount._4 + count * weight))
            n += 2
          }
          j += 1
        }
        val re1 = map.toArray.map(x => (x._1, x._2._1)).sortWith(_._2 > _._2).take(k).map(x => x._1 + ":" + x._2).mkString(" ")
        val re2 = map.toArray.map(x => (x._1, x._2._2)).sortWith(_._2 > _._2).take(k).map(x => x._1 + ":" + x._2).mkString(" ")
        val re3 = map.toArray.map(x => (x._1, x._2._3)).sortWith(_._2 > _._2).take(k).map(x => x._1 + ":" + x._2).mkString(" ")
        val re4 = map.toArray.map(x => (x._1, x._2._4)).sortWith(_._2 > _._2).take(k).map(x => x._1 + ":" + x._2).mkString(" ")
        srcNodesTopkFreq(idx) = (re1, re2, re3, re4)
      }
      println(s"partition $index: process, " +
        s"compute ${nodesIndex.length} nodes, " +
        s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")
    }
    //    (num1, num2)
    (srcNodes, srcNodesTopkFreq)
  }

}


object TopCandidateGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])]): TopCandidateGraphPartition = {
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

    new TopCandidateGraphPartition(index,
      srcNodesArray,
      csrPointer.toIntArray(),
      neighborsArray,
      new Array[(String, String, String, String)](srcNodesArray.length),
      neighborsArray.union(srcNodesArray).distinct,
      weights.toFloatArray()
    )
  }
}


