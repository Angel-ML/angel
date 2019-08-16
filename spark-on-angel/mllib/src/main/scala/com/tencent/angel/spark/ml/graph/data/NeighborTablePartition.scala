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
package com.tencent.angel.spark.ml.graph.data

import com.tencent.angel.spark.ml.graph.NeighborTableModel
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class NeighborTablePartition[@specialized(
  Byte, Boolean, Short, Int, Long, Float, Double) ED: ClassTag](isDirected: Boolean,
                                                                var partitionID: PartitionId,
                                                                var srcIds: Array[VertexId],
                                                                var neighbors: Array[Array[VertexId]],
                                                                var edgeAttrs: Array[Array[ED]]) extends Serializable {

  private def this(isDirected: Boolean) = this(isDirected, -1, null, null, null)

  private def this() = this(false)

  lazy val size: Int = srcIds.length

  lazy val numVertices: Long = srcIds.length

  lazy val numEdges: Long = neighbors.map(_.length).sum

  lazy val stats: GraphStats = {
    var minVertex = Long.MaxValue
    var maxVertex = Long.MinValue
    (0 until numVertices.toInt).foreach{ pos =>
      minVertex = minVertex min srcIds(pos) min neighbors(pos).head
      maxVertex = maxVertex max srcIds(pos) max neighbors(pos).last
    }
    GraphStats(minVertex, maxVertex, numVertices, numEdges)
  }

  private def fromNeighborRDD(data: Iterator[NeighborTable[ED]]): this.type = {
    val localSrcs = new ArrayBuffer[VertexId]
    val localNeighbors = new ArrayBuffer[Array[VertexId]]
    val localAttrs = new ArrayBuffer[Array[ED]]
    data.foreach{ nt =>
      localSrcs += nt.srcId
      localNeighbors += nt.neighborIds
      localAttrs += nt.attrs
    }
    srcIds = localSrcs.toArray
    neighbors = localNeighbors.toArray
    edgeAttrs = localAttrs.toArray
    this
  }

  def takeBatch(from: Int, length: Int): Array[NeighborTable[ED]] = {
    val length2 = length min (size - from)
    val rec = new Array[NeighborTable[ED]](length2)
    (0 until length2).foreach { pos =>
      rec(pos) = NeighborTable(srcIds(from + pos),
        neighbors(from + pos),
        if (edgeAttrs == null) null else edgeAttrs(from + pos))
    }
    rec
  }

  def iterator: Iterator[NeighborTable[ED]] = new Iterator[NeighborTable[ED]] {
    private[this] val instance = new NeighborTable[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < NeighborTablePartition.this.size

    override def next(): NeighborTable[ED] = {
      instance.srcId = srcIds(pos)
      instance.neighborIds = neighbors(pos)
      instance.attrs = if(edgeAttrs == null || edgeAttrs.isEmpty) null else edgeAttrs(pos)
      pos += 1
      instance
    }
  }

  def batchIterator(batchSize: Int): Iterator[Array[NeighborTable[ED]]] =
    BatchIter(iterator, batchSize)

  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, NeighborTablePartition.this.size))
    }

    override def hasNext: Boolean = {
      index < NeighborTablePartition.this.size
    }
  }

  def testPS(psModel: NeighborTableModel, num: Int): Boolean = {
    var correct = true
    assert(num <= size, s"the size of partition $size < $num")
    // check neighbor table
    for (i <- 0 until num - 1) {
      val srcId1 = srcIds(i)
      val srcId2 = srcIds(i + 1)
      val neighbors1 = neighbors(i)
      val neighbors2 = neighbors(i + 1)
      val trueNum = ArrayUtils.intersectCount(neighbors1, neighbors2)
      val fromPS = psModel.getLongNeighborTable(Array(srcId1, srcId2))
      val psNum = ArrayUtils.intersectCount(fromPS.get(srcId1), fromPS.get(srcId2))
      println(s"friends of $srcId1 = ${neighbors1.length} [RDD] ${fromPS.get(srcId1).length} [PS], " +
        s"friends of $srcId2 = ${neighbors2.length} [RDD] ${fromPS.get(srcId2).length} [PS], " +
        s"common friends = $trueNum [RDD] $psNum [PS]")
      if (correct && trueNum != psNum)
        correct = false
    }
    correct
  }

  def calLinkPrediction(psModel: NeighborTableModel): Iterator[Edge[LinkPredictionMetric]] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()

    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      println(s"partition $partitionID: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[Long]] = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      (from until to).foreach { pos =>
        numSrcNodes += 1
        localNeighborTable.put(srcIds(pos), neighbors(pos))
        pullNodes ++= neighbors(pos)
      }
      val srcNodes = localNeighborTable.keySet().toLongArray
      srcNodes.foreach { id => if(pullNodes.contains(id)) pullNodes.remove(id)}
      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = psModel.getLongNeighborTable(pullNodes.toArray)
      localNeighborTable.putAll(psNeighborsTable)
      psNeighborsTable.clear()
      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size
      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)
        srcNeighbors.flatMap { dst =>
          val metric = LinkPredictionMetric.getMetric(localNeighborTable, src, dst)
          Iterator.single(Edge(src, dst, metric))
        }
      }
    }
  }

  def calTriangleUndirected(psModel: NeighborTableModel): Iterator[(Long, Long, Seq[(Long, Long)])] =  {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    val startTs = System.currentTimeMillis()

    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      println(s"partition $partitionID: last batch cost ${System.currentTimeMillis() - startTs} ms")
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[Long]] = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      (from until to).foreach { pos =>
        numSrcNodes += 1
        localNeighborTable.put(srcIds(pos), neighbors(pos))
        pullNodes ++= neighbors(pos)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = psModel.getLongNeighborTable(pullNodes.toArray)

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src).filter(_ > src)
        val records = new mutable.ListBuffer[(Long, Long)]()
        val count: Long = srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst)
                             else psNeighborsTable.get(dst)

          val commFriends = dstNeighbors.filter(_ > dst).intersect(srcNeighbors)

          commFriends.foreach{w => records += ((w, 1L))}
          records += ((dst, commFriends.length))
          records += ((src, commFriends.length))

          Iterator.single(commFriends.length)
        }.sum

        // count is the deduplicated #triangles
        Iterator.single((src, count, records));
      }
    }
  }

}

object NeighborTablePartition {

  def fromNeighborTableRDD[ED: ClassTag](data: RDD[NeighborTable[ED]],
                                         isDirected: Boolean = false): RDD[NeighborTablePartition[ED]] = {
    data.mapPartitionsWithIndex { case (partId, iter) =>
      val localSrcs = new ArrayBuffer[VertexId]
      val localNeighbors = new ArrayBuffer[Array[VertexId]]
      val localAttrs = new ArrayBuffer[Array[ED]]
      iter.foreach{ item =>
        localSrcs += item.srcId
        localNeighbors += item.neighborIds
        if (item.attrs != null)
          localAttrs += item.attrs
      }
      Iterator.single(
        new NeighborTablePartition[ED](
          isDirected,
          partId,
          localSrcs.toArray,
          localNeighbors.toArray,
          if (localAttrs.isEmpty) null else localAttrs.toArray)
      )
    }
  }

}
