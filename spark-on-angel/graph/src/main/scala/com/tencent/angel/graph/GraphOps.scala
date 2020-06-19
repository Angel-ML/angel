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
package com.tencent.angel.graph

import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage
import com.tencent.angel.ml.math2.vector.IntLongVector
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult
import com.tencent.angel.graph.utils.element.{GraphStats, NeighborTable, NeighborTablePartition}
import com.tencent.angel.graph.psf.pagerank.GetNodes
import com.tencent.angel.graph.utils.element.Element.VertexId
import com.tencent.angel.spark.models.PSVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GraphOps {

  def buildOutDegreeTable(edges: RDD[(VertexId, VertexId)], partitionNum: Int): RDD[(VertexId, Int)] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, outNeighbors) =>
          Iterator.single(
            src,
            outNeighbors.toArray.count(_ != src))
        }
      } else {
        Iterator.empty
      }
    }
  }

  def buildVertexDegreeTable(edges: RDD[(VertexId, VertexId)], partitionNum: Int): RDD[(VertexId, Int)] = {
    // merge bi-directional edges then duplicate edges to make an undirected graph
    edges.flatMap { case (src, dst) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) {
        Iterator(((src, dst), (0, 0)))
      } else {
        Iterator(((dst, src), (1, 1)))
      }
    }.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (edge, seq) =>
          // emit bi-directional edges
          Array((edge._1, edge._2), (edge._2, edge._1)).iterator
        }
      } else {
        Iterator.empty
      }
    }.groupByKey(partitionNum).map { f => (f._1, f._2.size) }
  }

  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String
               ): RDD[(VertexId, VertexId)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else
          Iterator.single((row.getLong(0), row.getLong(1)))
      }
    }
  }

  def loadEdgesWithAttr[@specialized(
    Byte, Boolean, Short, Int, Long, Float, Double) ED: ClassTag](dataset: Dataset[_],
                                                                  srcNodeIdCol: String,
                                                                  dstNodeIdCol: String,
                                                                  attrCol: String): RDD[(VertexId, (VertexId, ED))] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol, attrCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else {
          val attr = row.get(2).asInstanceOf[ED]
          Iterator.single((row.getLong(0), (row.getLong(1), attr)))
        }
      }
    }
  }

  def edgesToNeighborTable(edges: RDD[(Long, Long)],
                           partitionNum: Int): RDD[NeighborTable[Object]] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(NeighborTable(src, group.toArray.distinct.filter(_ != src).sorted))
        }
      } else {
        Iterator.empty
      }
    }
  }

  /**
    * we put the smaller nodeId in front, and generate edges with flag
    * (0,0) encodes src < dst
    * (1,1) encodes src > dst
    * After that, we will merge bidirectional edges with reduceByKey
    *
    * @param edges
    * @param partitionNum
    * @return
    */
  def edgesToNeighborTableWithByteTags(edges: RDD[(Long, Long)], partitionNum: Int): RDD[NeighborTable[Byte]] = {
    // assume there is no redundant edges
    edges.flatMap { case (src, dst) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) { // small node comes first, use tag to denote direction
        Iterator(((src, dst), (0, 0))) // src -> dst, tag: 0
      } else {
        Iterator(((dst, src), (1, 1))) // dst <- src, tag: 1
      }
    }.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (edge, seq) =>
          val arr = seq.toArray
          if (arr.length == 1) {
            val tup = arr(0)
            if (tup._1 == 0 && tup._2 == 0) {
              Iterator((edge._1, (edge._2, 0.toByte))) // edge._1 < edge._2, edge._1 -> edge._2
            } else {
              Iterator((edge._1, (edge._2, 1.toByte))) // edge._1 < edge._2, edge._1 <- edge._2
            }
          } else {
            Iterator((edge._1, (edge._2, 2.toByte))) // edge._1 < edge._2, edge._1 <-> edge._2
          }
        }
      } else {
        Iterator.empty
      }
    }.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, seq) =>
          val array = seq.toArray.sorted // sort by nodeId
        val neighbors = new ArrayBuffer[VertexId](array.length)
          val attrs = new ArrayBuffer[Byte](array.length)
          // process edges in seq
          for (e <- array) {
            neighbors += e._1
            attrs += e._2
          }
          Iterator.single(NeighborTable(
            src,
            neighbors.toArray,
            attrs.toArray))
        }
      } else {
        Iterator.empty
      }
    }

  }


  /**
    *
    * @param edges
    * @param partitionNum
    * @return
    */
  def edgesToNeighborTableWithBytes(edges: RDD[(Long, Long)], partitionNum: Int): (RDD[NeighborTable[Byte]], RDD[NeighborTable[Byte]]) = {
    // assume there is no redundant edges
    val edgeType = edges.flatMap { case (src, dst) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) { // small node comes first, use tag to denote direction
        Iterator(((src, dst), (0, 0))) // src -> dst, tag: 0
      } else {
        Iterator(((dst, src), (1, 1))) // dst <- src, tag: 1
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 & b._2),partitionNum)

    //give each edge a tag [src < dst ,0],[src > dst,1] ,[src <-> dst, 2]
    // use extreme strategy
    val edgeTag = edgeType.map { case ((src, dst), (cnt, dir)) =>
      if (cnt >= 1 && dir == 1) {
        (src, (dst, 1.toByte))
      } else if (cnt >= 1 && dir == 0) {
        (src, (dst, 2.toByte))
      } else {
        (src, (dst, 0.toByte))
      }
    }

    // neighbors should be complete on ps
    val edgeDouble = edgeTag.flatMap { case (srcId, (dstId, tag)) =>
      Iterator((srcId, (dstId, tag)), (dstId, (srcId, tag)))
    }

    // neighbors on spark worker only need extreme neighbors
    val neighborForWorker = edgeTag.groupByKey().map {
      case (src, seq) =>
        // sort by nodeId
        val seqSorted = seq.toArray.sorted
        val (neighbors, tags) = seqSorted.unzip
        NeighborTable(src, neighbors, tags)
    }

    val neighborTable = edgeDouble.groupByKey()
      .map { case (src, seq) =>
        // sort by nodeId
        val seqSorted = seq.toArray.sorted
        val (neighbors, tags) = seqSorted.unzip
        NeighborTable(src, neighbors, tags)
      }

    (neighborForWorker, neighborTable)
  }


  def edgesToNeighborTableComplete(edges: RDD[(Long, Long)], partitionNum: Int): RDD[NeighborTable[Byte]] = {

    // assume there is no redundant edges
    val edgeType = edges.flatMap { case (src, dst) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) { // small node comes first, use tag to denote direction
        Iterator(((src, dst), (0, 0))) // src -> dst, tag: 0
      } else {
        Iterator(((dst, src), (1, 1))) // dst <- src, tag: 1
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 & b._2), partitionNum)

    //give each edge a tag [src < dst ,0],[src > dst,1] ,[src <-> dst, 2]
    // use extreme strategy
    val edgeTag = edgeType.map { case ((src, dst), (cnt, dir)) =>
      if (cnt >= 1 && dir == 1) {
        (src, (dst, 1.toByte))
      } else if (cnt >= 1 && dir == 0) {
        (src, (dst, 2.toByte))
      } else {
        (src, (dst, 0.toByte))
      }
    }

    // neighbors should be complete on ps
    val edgeDouble = edgeTag.flatMap { case (srcId, (dstId, tag)) =>
      Iterator((srcId, (dstId, tag)), (dstId, (srcId, tag)))
    }

    edgeDouble.groupByKey()
      .map { case (src, seq) =>
        // sort by nodeId
        val seqSorted = seq.toArray.sorted
        val (neighbors, tags) = seqSorted.unzip
        NeighborTable(src, neighbors, tags)
      }
  }

  def edgesToNeighbor(edges: RDD[(Long, Long)], partitionNum: Int): RDD[(Long, Iterable[(Long, Byte)])] = {
    // assume there is no redundant edges
    val edgeType = edges.flatMap { case (src, dst) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) { // small node comes first, use tag to denote direction
        Iterator(((src, dst), (0, 0))) // src -> dst, tag: 0
      } else {
        Iterator(((dst, src), (1, 1))) // dst <- src, tag: 1
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 & b._2),partitionNum)

    val edgeTag = edgeType.map { case ((src, dst), (cnt, dir)) =>
      if (cnt >= 1 && dir == 1) {
        (src, (dst, 1.toByte))
      } else if (cnt >= 1 && dir == 0) {
        (src, (dst, 2.toByte))
      } else {
        (src, (dst, 0.toByte))
      }
    }

    val edgeDouble = edgeTag.flatMap { case (srcId, (dstId, tag)) =>
      Iterator((srcId, (dstId, tag)), (dstId, (srcId, tag)))
    }

    edgeDouble.groupByKey()
  }

  def edgesForTriangleWithBytes(edges: RDD[(Long, Long)], partitionNum: Int): RDD[NeighborTable[Byte]] = {
    // assume there is no redundant edges
    val edgeType = edges.flatMap { case (src, dst) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) { // small node comes first, use tag to denote direction
        Iterator(((src, dst), (0, 0))) // src -> dst, tag: 0
      } else {
        Iterator(((dst, src), (1, 1))) // dst <- src, tag: 1
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 & b._2),partitionNum)

    val edgeTag = edgeType.map { case ((src, dst), (cnt, dir)) =>
      if (cnt >= 1 && dir == 1) {
        (src, (dst, 1.toByte))
      } else if (cnt >= 1 && dir == 0) {
        (src, (dst, 2.toByte))
      } else {
        (src, (dst, 0.toByte))
      }
    }


    val neighborForWorker = edgeTag.groupByKey().map {
      case (src, seq) =>
        // sort by nodeId
        val seqSorted = seq.toArray.sorted
        val (neighbors, tags) = seqSorted.unzip
        NeighborTable(src, neighbors, tags)
    }


    neighborForWorker
  }

  def buildNeighborTablePartition[ED: ClassTag](data: RDD[NeighborTable[ED]],
                                                isDirected: Boolean = false): RDD[NeighborTablePartition[ED]] = {
    NeighborTablePartition.fromNeighborTableRDD(data, isDirected)
  }

  def getStats[ED: ClassTag](data: RDD[NeighborTablePartition[ED]]): GraphStats = {
    data.map(_.stats).reduce(_ + _)
  }

  /**
    * Get all valid node ids from PS by mini-batch
    */
  def getNodesByPartitions(vector: PSVector, partitionIds: Array[Int], numBatch: Int): Array[Long] = {
    val batchSize = math.max(partitionIds.length / numBatch, 1)
    var start = 0
    val result = new ArrayBuffer[Long]()
    while (start < partitionIds.length) {
      val func = new GetNodes(vector.poolId, partitionIds.slice(start, start + batchSize))
      result ++= vector.psfGet(func).asInstanceOf[GetRowResult].getRow.asInstanceOf[IntLongVector]
        .getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
      start += batchSize
    }
    result.toArray
  }

}
