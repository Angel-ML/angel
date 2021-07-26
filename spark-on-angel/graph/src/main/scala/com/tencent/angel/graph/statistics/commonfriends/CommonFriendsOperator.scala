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
package com.tencent.angel.graph.statistics.commonfriends

import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CommonFriendsOperator {
  def runEdgePartition(iter: Iterator[(Long, Long)], partitionId: Int, batchSize: Int, model: SimpleNeighborTableModel,
                       maxComFriendsNum: Int = Int.MaxValue): Iterator[Row] = {
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val edgeBuffer: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      batchIter.foreach { curEdge =>
        pullNodes.add(curEdge._1)
        pullNodes.add(curEdge._2)
        edgeBuffer += curEdge
      }
      val beforePullTs = System.currentTimeMillis()
      val neighborsNodesMap = model.getNeighbors(pullNodes.toArray)
      totalRowNum += batchSize
      totalPullNum += pullNodes.size
      println(s"partition $partitionId process $batchSize edges ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      edgeBuffer.toIterator.flatMap { case (src, dst) =>
        val srcNeighbors = neighborsNodesMap.get(src)
        val dstNeighbors = neighborsNodesMap.get(dst)
        Iterator.single(Row(src, dst, ArrayUtils.intersectCountWithLimits(srcNeighbors, dstNeighbors, maxComFriendsNum)))
      }
    }
  }

  def runNeighborPartition(iter: Iterator[(Long, Array[Long])], partitionId: Int, batchSize: Int, model: SimpleNeighborTableModel,
                           maxComFriendsNum: Int = Int.MaxValue): Iterator[Row] = {
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[Long]] = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      batchIter.foreach { case (src, neighbors) =>
        numSrcNodes += 1
        localNeighborTable.put(src, neighbors)
        if (localNeighborTable.containsKey(src))
          pullNodes ++= neighbors
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = model.getNeighbors(pullNodes.toArray)
      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size
      println(s"partition $partitionId: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      val srcNodes = localNeighborTable.keySet().toLongArray
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)
        srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst) else psNeighborsTable.get(dst)
          Iterator.single(Row(src, dst, ArrayUtils.intersectCountWithLimits(srcNeighbors, dstNeighbors, maxComFriendsNum)))
        }
      }
    }
  }

}
