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

import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CommonFriendsOperator {

  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String
               ): RDD[(Long, Long)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .filter(row => !row.anyNull)
      .mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1))
            Iterator.empty
          else
            Iterator.single(row.getLong(0), row.getLong(1))
        }
      }
  }

  def loadCompressedEdges(dataset: Dataset[_],
                          srcNodeIdCol: String,
                          dstNodeIdCol: String,
                          compressCol: String
                         ): RDD[(Long, Long)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol, compressCol).rdd
      .filter(row => !row.anyNull)
      .mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1))
            Iterator.empty
          else if (row.getFloat(2) == 1)
            Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0)))
          else
            Iterator.single(row.getLong(0), row.getLong(1))
        }
      }
  }

  def edges2NeighborTable(edges: RDD[(Long, Long)],
                          partitionNum: Int): RDD[(Long, Array[Long])] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.filter(_ != src).sorted)
        }
      } else {
        Iterator.empty
      }
    }
  }

  def statsByNeighborTable(neighborTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      iter.foreach { case (src, neighbors) =>
        min = math.min(min, math.min(src, neighbors.head))
        max = math.max(max, math.max(src, neighbors.last))
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single((min, max, numNodes, numEdges))
    }.reduce { case (c1: (Long, Long, Long, Long), c2: (Long, Long, Long, Long)) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4)
    }
  }

  def testPS(neighborsRDD: RDD[(Long, Array[Long])], psModel: CommonFriendsPSModel, num: Int): Unit = {
    println(s"======test PS======")
    val nodeIds = neighborsRDD.take(num).map(_._1)
    val nodeNeighbors = psModel.getLongNeighborTable(nodeIds)
    val iter = nodeNeighbors.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      println(s"node id = ${entry.getLongKey}, neighbors = ${entry.getValue.mkString(",")}")
    }
  }

  def checkValid(neighborsRDD: RDD[(Long, Array[Long])], psModel: CommonFriendsPSModel, num: Int): Boolean = {
    println(s"======check correctness======")
    val sampled = neighborsRDD.takeSample(false, num)
    var correct = true
    for (i <- 0 until sampled.length - 1) {
      val item1 = sampled(i)
      val item2 = sampled(i + 1)
      val trueNum = ArrayUtils.intersectCount(item1._2, item2._2)
      val fromPS = psModel.getLongNeighborTable(Array(item1._1, item2._1))
      val psNum = ArrayUtils.intersectCount(fromPS.get(item1._1), fromPS.get(item2._1))
      println(s"friends of ${item1._1} = ${item1._2.length} [RDD] ${fromPS.get(item1._1).length} [PS], " +
        s"friends of ${item2._1}: ${item2._2.length} [RDD] ${fromPS.get(item2._1).length} [PS], " +
        s"common friends = $trueNum [RDD] $psNum [PS]")
      if (correct && trueNum != psNum)
        correct = false
    }
    correct
  }

  def runEdgePartition(iter: Iterator[(Long, Long)], partitionId: Int, psModel: CommonFriendsPSModel): Iterator[Row] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
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
      val neighborsNodesMap = psModel.getLongNeighborTable(pullNodes.toArray)
      totalRowNum += batchSize
      totalPullNum += pullNodes.size
      println(s"partition $partitionId process $batchSize edges ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      edgeBuffer.toIterator.flatMap { case (src, dst) =>
        val srcNeighbors = neighborsNodesMap.get(src)
        val dstNeighbors = neighborsNodesMap.get(dst)
        Iterator.single(Row(src, dst, ArrayUtils.intersectCount(srcNeighbors, dstNeighbors)))
      }
    }
  }

  def runNeighborPartition(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: CommonFriendsPSModel): Iterator[Row] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
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
      val psNeighborsTable = psModel.getLongNeighborTable(pullNodes.toArray)
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
          Iterator.single(Row(src, dst, ArrayUtils.intersectCount(srcNeighbors, dstNeighbors)))
        }
      }
    }
  }

  //  def runNeighborPartitionTag(iter: Iterator[(Long, Array[(Long,Byte)])], partitionId: Int, psModel: CommonFriendsPSModel): Iterator[Row] = {
  //    val batchSize = psModel.neighborTable.param.pullBatchSize
  //    var totalRowNum = 0
  //    var totalPullNum = 0
  //    var startTs = System.currentTimeMillis()
  //    BatchIter(iter, batchSize).flatMap { batchIter =>
  //      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
  //      startTs = System.currentTimeMillis()
  //      var numSrcNodes = 0
  //      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  //      val localNeighborTable: Long2ObjectOpenHashMap[Array[Long]] = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
  //      batchIter.foreach { case (src, neighbors) =>
  //        numSrcNodes += 1
  //        val (nbrs,nbrsTag) =  neighbors.unzip
  //        localNeighborTable.put(src,nbrs)
  //        if (localNeighborTable.containsKey(src))
  //          pullNodes ++= nbrs
  //      }
  //      val beforePullTs = System.currentTimeMillis()
  //      val psNeighborsTable = psModel.getLongNeighborTable(pullNodes.toArray)
  //      totalRowNum += numSrcNodes
  //      totalPullNum += pullNodes.size
  //      println(s"partition $partitionId: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
  //        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
  //        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
  //      val srcNodes = localNeighborTable.keySet().toLongArray
  //      srcNodes.flatMap { src =>
  //        val srcNeighbors = localNeighborTable.get(src)
  //        srcNeighbors.flatMap { dst =>
  //          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst) else psNeighborsTable.get(dst)
  //          val commonNbrsCount = ArrayUtils.intersectCount(srcNeighbors, dstNeighbors)
  //          if()
  //          Iterator.single(Row(src, dst,commonNbrsCount ))
  //        }
  //      }
  //    }
  //  }

}
