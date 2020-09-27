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

package com.tencent.angel.graph.embedding.deepwalk

import com.tencent.angel.graph.psf.neighbors.samplebyaliastable.samplealiastable.NeighborsAliasTableElement
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class DeepWalkGraphPartition(index: Int, srcNodesArray: Array[Long], srcNodesSamplePaths: Array[Array[Long]], batchSize: Int) {

  def process(model: DeepWalkPSModel, iteration: Int): DeepWalkGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")
    val rnd = new Random()
    //sample nodes path batch by batch
    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      //the tail nodes set of paths
      val pullNodes = srcNodesSamplePaths.slice(nodesIndex.head, nodesIndex.last + 1)
        .map(a => (a.last, 1)).groupBy(_._1).map(t => (t._1, t._2.size))
      val (nodes, count) = pullNodes.unzip

      //pull nodes neighbors
      val beforeSample = System.currentTimeMillis()
      val nodesToNeighboes = model.getSampledNeighbors(model.edgesPsMatrix, nodes.toArray, count.toArray)
      println(s"partition $index, iter $iteration, sampleTime: ${System.currentTimeMillis() - beforeSample} ms")

      //process each node
      for (idx <- nodesIndex) {
        val oldPath = srcNodesSamplePaths(idx) // the old path of idx node
        val oldPathTail = oldPath.last // the tail node of path
        val tailNeighbors = nodesToNeighboes.get(oldPathTail) //the neighbors of tail node
        if (tailNeighbors.nonEmpty) {
          val sampleFromNeighbors = tailNeighbors(rnd.nextInt(tailNeighbors.length)) // sample a node randomly from tail node's neighbors
          srcNodesSamplePaths(idx) = Array.concat(oldPath, Array(sampleFromNeighbors)) // merge old path and sample node
        }

      }
    }
    println(s"partition $index: ---------- iteration $iteration terminated ----------")
    new DeepWalkGraphPartition(index, srcNodesArray, srcNodesSamplePaths, batchSize)

  }

  def save(): Array[Array[Long]] =
    srcNodesSamplePaths
}

object DeepWalkGraphPartition {
  def initPSMatrixAndNodePath(model: DeepWalkPSModel, index: Int, iterator: Iterator[(Long, Array[Long], Array[Float], Array[Int])], batchSize: Int): DeepWalkGraphPartition = {
    val srcNodes = new LongArrayList()
    iterator.sliding(batchSize, batchSize).foreach { pairs =>
      val nodeId2Neighbors = new Long2ObjectOpenHashMap[NeighborsAliasTableElement](pairs.length)
      pairs.foreach { case (src, neighbors, accept, alias) =>
        val elem = new NeighborsAliasTableElement(neighbors, accept, alias)
        nodeId2Neighbors.put(src, elem)
        srcNodes.add(src)
      }
      model.initNodeNei(nodeId2Neighbors)
      nodeId2Neighbors.clear()
    }
    initNodePaths(index,srcNodes.toLongArray(),batchSize)
  }

  def initNodePaths(index: Int, iterator: Array[Long], batchSize: Int): DeepWalkGraphPartition = {
    val srcNodesSamplePaths = ArrayBuffer[Array[Long]]()
    iterator.foreach { node =>
      srcNodesSamplePaths.append(Array(node))
    }
    new DeepWalkGraphPartition(index, iterator, srcNodesSamplePaths.toArray, batchSize)
  }
}
