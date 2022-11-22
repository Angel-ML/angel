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
package com.tencent.angel.graph.statistics.lccUndirected

import com.tencent.angel.graph.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.utils.collection.OpenHashMap
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LccOperator {

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
        else { // make src < dst
          if (row.getLong(0) < row.getLong(1)) Iterator.single(row.getLong(0), row.getLong(1))
          else Iterator.single(row.getLong(1), row.getLong(0))
        }
      }
    }
  }

  def calcDegree(neighbors: RDD[(Long, Array[Long])], batchSize: Int): RDD[(Long, Int)] = {
    neighbors.flatMap(x => Array((x._1, x._2.length)) ++ x._2.map(y => (y, 1))).reduceByKey(_ + _)
  }

  def initDegree(minId: Long, maxId: Long, degree: RDD[(Long, Int)], psPartitionNum: Int, batchSize: Int): PSVector = {
    val matrix = new MatrixContext("degree", 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    matrix.setMaxColNumInBlock((maxId - minId) / psPartitionNum)
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("degree").getId
    val degreeVec = new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType)

    degree.foreachPartition { iter =>
      iter.sliding(batchSize, batchSize).foreach { pairs =>
        val msgs = VFactory.sparseLongKeyIntVector(maxId)
        pairs.foreach { case (node, num) => msgs.set(node, num) }
        degreeVec.update(msgs)
        println(s"init ${pairs.length} node-degree maps.")
        msgs.clear()
      }
    }
    degreeVec
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
        val degree = srcNeighbors.length
        var triangles = srcNeighbors.map { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst) else psNeighborsTable.get(dst)
          ArrayUtils.intersectCount(srcNeighbors, dstNeighbors)
        }.sum
        assert(triangles % 2 == 0, s"triangle count error, num triangles: $triangles.")
        triangles = triangles / 2
        val lcc = if (degree < 2) 0f else triangles.toFloat / (degree * (degree - 1) / 2)
        Iterator(Row(src, triangles, lcc))
      }
    }
  }

  def runNeighborPartition_(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: CommonFriendsPSModel): Iterator[(Long, Int)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"calc triangle: partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
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
      println(s"calc triangle: partition $partitionId: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      val srcNodes = localNeighborTable.keySet().toLongArray
      val msgMap = new OpenHashMap[Long, Int]()
      srcNodes.foreach { src =>
        val srcNeighbors = localNeighborTable.get(src)
        srcNeighbors.foreach { dst =>
          val dstNeighbors = {
            if (localNeighborTable.containsKey(dst))
              localNeighborTable.get(dst)
            else psNeighborsTable.get(dst)
          }
          val comFriends = srcNeighbors.intersect(dstNeighbors)
          if (comFriends.length > 0) {
            comFriends.foreach(x => msgMap.changeValue(x, 1, a => a + 1))
            msgMap.changeValue(src, comFriends.length, a => a + comFriends.length)
            msgMap.changeValue(dst, comFriends.length, a => a + comFriends.length)
          } else {
            msgMap.changeValue(src, 0, a => a)
            msgMap.changeValue(dst, 0, a => a)
          }
        }
      }
      msgMap.toArray.toIterator
    }
  }

  def runNeighborPartition_1(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: CommonFriendsPSModel): Iterator[(Long, Int)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"calc triangle: partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
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
      println(s"calc triangle: partition $partitionId: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      val srcNodes = localNeighborTable.keySet().toLongArray
      val coms = srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)
        srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst) else psNeighborsTable.get(dst)
          val comFriends = intersect(srcNeighbors, dstNeighbors)
          val temp = comFriends.map(x => (x, 1))
          temp.append((src, comFriends.length))
          temp.append((dst, comFriends.length))
          temp.toIterator
        }
      }
      coms.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum))
    }
  }

  def calcLcc(index: Int, iter: Iterator[(Long, Int)], degreeVec: PSVector, batchSize: Int): Iterator[Row] = {
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"calc lcc: partition $index: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = batchIter.map(_._1)
      val beforePullTs = System.currentTimeMillis()
      val degreeMap = degreeVec.pull(pullNodes).asInstanceOf[LongIntVector]
      println(s"calc lcc: partition $index: " +
        s"pull ${pullNodes.length} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      batchIter.flatMap { case (src, triangleNum) =>
        val degree = degreeMap.get(src)
        val lcc = if (degree < 2) 0f else triangleNum.toFloat / (degree * (degree - 1) / 2)
        val es = degree.toFloat - (2 * triangleNum / degree.toFloat)
        Iterator.single(Row(src, triangleNum, lcc, es))
      }
    }
  }

  def intersect(array1: Array[Long], array2: Array[Long]): ArrayBuffer[Long] = {
    val re = new ArrayBuffer[Long]()
    var i = 0
    var j = 0
    while (i < array1.length && j < array2.length) {
      if (array1(i) < array2(j))
        i += 1
      else if (array1(i) > array2(j))
        j += 1
      else {
        re.append(array1(i))
        i += 1
        j += 1
      }
    }
    re
  }
}
