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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.spark.ml.graph.algo.rank.clusterrank.NeighborEdgesModel
import com.tencent.angel.spark.ml.graph.algo.statistics.triangle.TriangleCountingDirected
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.spark.ml.graph.{NeighborTableModel, OutDegreeModel}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import NeighborTablePartition._
import com.tencent.angel.spark.ml.graph.data.CheckMotif

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
    (0 until numVertices.toInt).foreach { pos =>
      minVertex = minVertex min srcIds(pos) min neighbors(pos).head
      maxVertex = maxVertex max srcIds(pos) max neighbors(pos).last
    }
    GraphStats(minVertex, maxVertex, numVertices, numEdges)
  }

  private def fromNeighborRDD(data: Iterator[NeighborTable[ED]]): this.type = {
    val localSrcs = new ArrayBuffer[VertexId]
    val localNeighbors = new ArrayBuffer[Array[VertexId]]
    val localAttrs = new ArrayBuffer[Array[ED]]
    data.foreach { nt =>
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
      instance.attrs = if (edgeAttrs == null || edgeAttrs.isEmpty) null else edgeAttrs(pos)
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
    if (num > size) {
      println(s"partition $partitionID: size $size < check num $num, check $size nodes only")
    }
    val checkNum = math.min(num, size)
    // check neighbor table
    for (i <- 0 until checkNum - 1) {
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
      srcNodes.foreach { id => if (pullNodes.contains(id)) pullNodes.remove(id) }
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

  def generateLongMsg(hashMap: mutable.HashMap[VertexId, Long], msg: (VertexId, Long)): Unit = {
    if (hashMap.contains(msg._1)) {
      val count = hashMap(msg._1)
      hashMap.put(msg._1, count + msg._2)
    } else {
      hashMap.put(msg._1, msg._2)
    }

  }

  def calSwing(psModel:NeighborTableModel,alpha:Float):Iterator[(Long,Array[(Long,Float)])] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = psNbrTable.get(src)
        val items2Items = new Array[(Long,Array[Long])](srcNbrs.size)
        var i = 0
        srcNbrs.map { case (dst, tag) =>
          items2Items(i) = (dst,psNbrTable.get(dst).map(_._1))
          i += 1
        }
        val res = SwingCompute.Swing(src,items2Items,alpha)
       Iterator(res)
      }
    }
  }
  
  def calWeightedSwing(psModel:NeighborTableModel, alpha:Float):Iterator[(Long,Array[(Long,Float)])] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L
    
    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Float)]](batchSize)
      
      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Float)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Float]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }
      
      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Float](pullNodes.toArray) //get neighborsTable from ps
      
      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size
      
      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      
      val srcNodes = localNbrTable.keySet().toLongArray
      
      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = psNbrTable.get(src)
        val items2Items = new Array[((Long, Float),Array[(Long, Float)])](srcNbrs.size)
        var i = 0
        srcNbrs.foreach { case (dst, tag) =>
          items2Items(i) = ((dst, tag) ,psNbrTable.get(dst))
          i += 1
        }
        val res = SwingCompute.WeightedSwing(src,items2Items,alpha)
        Iterator(res)
      }
    }
  }
  
  def calTriangleUndirected(psModel: NeighborTableModel): Iterator[(VertexId, Long, Int, Seq[(Long, Long)])] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    val startTs = System.currentTimeMillis()

    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      println(s"partition $partitionID: last batch cost ${System.currentTimeMillis() - startTs} ms")
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[VertexId]] = new Long2ObjectOpenHashMap[Array[VertexId]](batchSize)
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
        val degree = localNeighborTable.get(src).length
        val srcNeighbors = localNeighborTable.get(src).filter(_ > src)
        val msgMap = new mutable.HashMap[Long, Long]()
        //        val records = new mutable.ListBuffer[(Long, Long)]()
        val count: Long = srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst)
          else psNeighborsTable.get(dst)

          val commFriends = dstNeighbors.filter(_ > dst).intersect(srcNeighbors)

          commFriends.foreach { w => generateLongMsg(msgMap, (w, 1L)) }

          generateLongMsg(msgMap, (dst, commFriends.length.toLong))
          generateLongMsg(msgMap, (src, commFriends.length.toLong))

          //          commFriends.foreach{w => records += ((w, 1L))}
          //          records += ((dst, commFriends.length))
          //          records += ((src, commFriends.length))

          Iterator.single(commFriends.length)
        }.sum

        val messages = new mutable.ArrayBuffer[(Long, Long)](msgMap.size)
        msgMap.foreach(kv => messages += kv)
        msgMap.clear()

        // count is the deduplicated #triangles
        Iterator.single((src, count, degree, messages));
      }
    }
  }

  private[NeighborTablePartition]
  class NodeDeg(var outDeg: Int, var inDeg: Int) extends Serializable {
    override def hashCode(): Int = {
      outDeg * 10 + inDeg
    }

    override def equals(obj: Any): Boolean = {
      if (!obj.isInstanceOf[NodeDeg]) {
        false
      } else {
        val other = obj.asInstanceOf[NodeDeg]
        outDeg == other.outDeg && inDeg == other.inDeg
      }
    }
  }

  def generateArrayMsg(hashMap: mutable.HashMap[VertexId, CounterTriangleDirected], msg: (VertexId, CounterTriangleDirected)): Unit = {
    if (hashMap.contains(msg._1)) {
      val sum = new CounterTriangleDirected(7)
      val v = hashMap(msg._1)
      sum(0) = v(0) + msg._2(0)
      sum(1) = v(1) + msg._2(1)
      sum(2) = v(2) + msg._2(2)
      sum(3) = v(3) + msg._2(3)
      sum(4) = v(4) + msg._2(4)
      sum(5) = v(5) + msg._2(5)
      sum(6) = v(6) + msg._2(6)

      hashMap.put(msg._1, sum)
    } else {
      hashMap.put(msg._1, msg._2)
    }
  }

  /**
    * Note: bidirectional edges are not merged for this method
    *
    * @param psModel
    * @return
    */
  def calNumEdgesInOutNeighbor(psModel: NeighborTableModel): Iterator[(VertexId, Long)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"calNumEdges: partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"calNumEdges: partition $partitionID: last batch total_time: ${endTs - startTs} ms, comp_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[VertexId]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[VertexId]] =
        new Long2ObjectOpenHashMap[Array[VertexId]](batchSize)

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
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)

        val total: Long = srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst)
          else psNeighborsTable.get(dst)

          if (dstNeighbors != null && dstNeighbors.nonEmpty) {
            // get the number of common nodes of srcNeighbors and dstNeighbors
            val commonNeighbors = ArrayUtils.intersectCount(srcNeighbors, dstNeighbors)
            Iterator.single(commonNeighbors)
          } else {
            Iterator.single(0)
          }
        }.sum

        // numEdges is the deduplicated number of triangles which src belongs to
        Iterator.single((src, total))
      }
    }

  }

  /**
    * Calculate the number of edges in in-neighbors and out-neighbors
    *
    * @param psModel
    * @return
    */
  def calNumEdgesInNeighbor(psModel: NeighborTableModel): Iterator[(VertexId, Long, Seq[(VertexId, Long)])] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[VertexId] = new mutable.HashSet[VertexId]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[(VertexId, ED)]] =
        new Long2ObjectOpenHashMap[Array[(VertexId, ED)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(VertexId, ED)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx)
          edgeWithAttrs += ((dst, attr))

        }
        localNeighborTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
      }

      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = psModel.getAttrLongNeighborTable[ED](pullNodes.toArray)

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src).filter(_._1 > src)
        val msgMap = new mutable.HashMap[VertexId, Long]()

        val total: Long = srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst._1)) localNeighborTable.get(dst._1)
          else psNeighborsTable.get(dst._1)

          if (dstNeighbors != null && dstNeighbors.nonEmpty) {
            // get the common nodes of srcNeighbors and dstNeighbors, and return the attribute on the edges
            val commonFriends = TriangleCountingDirected.intersect[ED](srcNeighbors, dstNeighbors)
            var sum = 0L

            // suppose the common neighbor is node u
            for (u <- commonFriends) {
              // a bi-directional edge should be seen as two edges, an in-edge and an out-edge, in LCC calculation
              if (u._2._2 == 2)
                generateLongMsg(msgMap, (src, 1L))
              if (u._2._1 == 2)
                generateLongMsg(msgMap, (dst._1, 1L))

              if (dst._2 == 2)
                generateLongMsg(msgMap, (u._1, 2L))
              else
                generateLongMsg(msgMap, (u._1, 1L))

              sum += 1
            }

            generateLongMsg(msgMap, (src, sum))
            generateLongMsg(msgMap, (dst._1, sum))
            Iterator.single(commonFriends.length)
          } else {
            Iterator.single(0)
          }
        }.sum

        val messages = new mutable.ArrayBuffer[(VertexId, Long)](msgMap.size)
        msgMap.foreach(kv => messages += kv)
        msgMap.clear()

        // total is the deduplicated number of triangles which src belongs to
        Iterator.single((src, total, messages))
      }
    }

  }

  def calTriangleDirected(psModel: NeighborTableModel): Iterator[(VertexId, Long, Seq[(VertexId, CounterTriangleDirected)])] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    //    val numNeighborTables: Int = batchSize * 100
    //    val neighborsCache = DegreeBasedCache.newInstance[VertexId, Array[(VertexId, ED)]](numNeighborTables)

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[(Long, ED)]] =
        new Long2ObjectOpenHashMap[Array[(Long, ED)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, ED)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx)
          edgeWithAttrs += ((dst, attr))

          //          if (!neighborsCache.contains(dst))
          //            pullNodes += dst

        }
        localNeighborTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
      }

      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = psModel.getAttrLongNeighborTable[ED](pullNodes.toArray)

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray
      //      val records = new ArrayBuffer[(VertexId, Long, Seq[(VertexId, CounterTriangleDirected)])](srcNodes.length)

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src).filter(_._1 > src)
        val msgMap = new mutable.HashMap[VertexId, CounterTriangleDirected]()

        val total: Long = srcNeighbors.flatMap { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst._1)) localNeighborTable.get(dst._1)
          else psNeighborsTable.get(dst._1)
          //            else if (neighborsCache.contains(dst._1)) neighborsCache.get(dst._1)
          //            else psNeighborsTable.get(dst._1)

          if (dstNeighbors != null && dstNeighbors.nonEmpty) {
            val srcDeg = new NodeDeg(0, 0)
            val dstDeg = new NodeDeg(0, 0)
            val uDeg = new NodeDeg(0, 0)
            // get the common nodes of srcNeighbors and dstNeighbors, and return the attribute on the edges
            val commonFriends = TriangleCountingDirected.intersect[ED](srcNeighbors, dstNeighbors)
            val sum = new CounterTriangleDirected(7)

            // suppose the common friend is node u,
            // the triangle is formed by edges (src, dst), (dst, u), (src, u)
            for (u <- commonFriends) {
              // count the out degree and in degree for src, dst and u
              uDeg.outDeg = 0; uDeg.inDeg = 0
              srcDeg.outDeg = 0; srcDeg.inDeg = 0
              dstDeg.outDeg = 0; dstDeg.inDeg = 0

              // (src, dst)
              dst._2 match {
                case 0 => srcDeg.outDeg += 1; dstDeg.inDeg += 1
                case 1 => srcDeg.inDeg += 1; dstDeg.outDeg += 1
                case 2 => srcDeg.outDeg += 1; srcDeg.inDeg += 1; dstDeg.outDeg += 1; dstDeg.inDeg += 1
              }

              // (src, u)
              u._2._1 match {
                case 0 => srcDeg.outDeg += 1; uDeg.inDeg += 1
                case 1 => srcDeg.inDeg += 1; uDeg.outDeg += 1
                case 2 => srcDeg.outDeg += 1; srcDeg.inDeg += 1; uDeg.outDeg += 1; uDeg.inDeg += 1
              }

              // (dst, u)
              u._2._2 match {
                case 0 => dstDeg.outDeg += 1; uDeg.inDeg += 1
                case 1 => dstDeg.inDeg += 1; uDeg.outDeg += 1
                case 2 => dstDeg.outDeg += 1; dstDeg.inDeg += 1; uDeg.outDeg += 1; uDeg.inDeg += 1
              }

              assert(srcDeg.outDeg < 3 && srcDeg.inDeg < 3)
              assert(dstDeg.outDeg < 3 && dstDeg.inDeg < 3)

              val hset = new mutable.HashSet[NodeDeg]()
              hset.add(srcDeg)
              hset.add(dstDeg)
              hset.add(uDeg)

              // map (srcDeg, dstDeg, uDeg) to [0,6]
              val temp = new NodeDeg(1, 1)
              for (deg <- hset) {
                temp.outDeg = temp.outDeg * deg.outDeg
                temp.inDeg = temp.inDeg * deg.inDeg
              }

              val idx = (temp.outDeg, temp.inDeg) match {
                case (0, 0) => 0
                case (2, 0) => 2
                case (0, 2) => 4
                case (1, 1) => 1
                case (2, 2) if hset.size == 3 => 3
                case (4, 4) => 5
                case (2, 2) if hset.size == 1 => 6
                case _ => 7
              }

              if (idx == 7) {
                throw new AngelException("Unexpected idx. outdeg: " + temp.outDeg + ", indeg: " + temp.inDeg)
              }

              // emit for common friends
              val uCount = new CounterTriangleDirected(7)
              uCount(idx) += 1
              generateArrayMsg(msgMap, (u._1, uCount))

              sum(idx) += 1
            }

            generateArrayMsg(msgMap, (src, sum))
            generateArrayMsg(msgMap, (dst._1, sum))

            Iterator.single(commonFriends.length)
          } else {

            val zero = Array[Int](0, 0, 0, 0, 0, 0, 0)
            generateArrayMsg(msgMap, (src, zero))
            generateArrayMsg(msgMap, (dst._1, zero))

            Iterator.single(0)
          }
        }.sum

        val messages = new mutable.ArrayBuffer[(Long, CounterTriangleDirected)](msgMap.size)
        msgMap.foreach(kv => messages += kv)
        msgMap.clear()

        // total is the deduplicated number of triangles which src belongs to
        Iterator.single((src, total, messages))
      }

      // update neighbors cache
      /*      for (nodeId <- pullNodes) {
              val neighbors = psNeighborsTable.get(nodeId)
              neighborsCache.put(nodeId, neighbors, neighbors.length)
            }

            records.iterator*/

    }
  }

  def calClusterRank(degreeModel: OutDegreeModel, neighborEdgesModel: NeighborEdgesModel): Iterator[Row] = {
    val batchSize = degreeModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"clusterRank: partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"clusterRank: partition $partitionID: last batch total_time: ${endTs - startTs} ms, comp_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val srcNeighborsBatch: mutable.HashSet[Long] = new mutable.HashSet[VertexId]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[VertexId]] =
        new Long2ObjectOpenHashMap[Array[VertexId]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        localNeighborTable.put(srcIds(pos), neighbors(pos))
        srcNeighborsBatch ++= neighbors(pos)
      }
      val beforePullTs = System.currentTimeMillis()

      totalRowNum += numSrcNodes
      totalPullNum += srcNeighborsBatch.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${srcNeighborsBatch.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray
      val node2NumEdgesInNeighbor = neighborEdgesModel.getNumEdges(srcNodes)
      val node2OutDegree = degreeModel.getOutDegrees(srcNeighborsBatch.toArray)


      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val kOut = node2OutDegree.get(src)
        val numEdges = node2NumEdgesInNeighbor.get(src)
        val lcc: Double = if (kOut > 1) numEdges.toDouble / (kOut * (kOut - 1)) else 0.0
        var sum: Long = 0L

        localNeighborTable.get(src).foreach(v => sum += (node2OutDegree.get(v) + 1))
        val clRank = math.pow(10.0, -lcc) * sum

        Iterator.single(Row(src, clRank))
      }
    }
  }

  /**
    * count each vertex's triangle-motif
    *
    * @param psModel
    * @return
    */
  def calMotifDirectedTriangle(psModel: NeighborTableModel): Iterator[(Long, Byte)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = localNbrTable.get(src)
        srcNbrs.flatMap { case (dst, tag) =>
          val dstNbrs = psNbrTable.get(dst)
          if (dstNbrs != null && dstNbrs.nonEmpty) {
            val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
            computeTriangleMotif(src, dst, tag, commonNbrs)
          } else {
            Iterator.empty
          }
        }
      }
    }
  }

  def calTwoEdgeMotifMatrix(psModel: NeighborTableModel): Iterator[(Long, Long, Byte)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = localNbrTable.get(src)

        srcNbrs.flatMap { case (dst, tag) =>
          val dstNbrs = psNbrTable.get(dst)
          if (dstNbrs.nonEmpty) {
            val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
            computeTwoEdgeMotifMatrix(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
          } else {
            Iterator.empty
          }
        }
      }
    }
  }


  def calMotifDirected(psModel: NeighborTableModel): Iterator[(Long, Byte)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = localNbrTable.get(src)

        srcNbrs.flatMap { case (dst, tag) =>
          val dstNbrs = psNbrTable.get(dst)
          if (dstNbrs.nonEmpty) {
            val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
            computeMotif(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
          } else {
            Iterator.empty
          }
        }
      }
    }
  }

  def calMotifDirectedCount(psModel: NeighborTableModel): Iterator[((Long, Byte), Int)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = psNbrTable.get(src)

        srcNbrs.filter(_._1 > src).flatMap { case (dst, tag) =>
          val dstNbrs = psNbrTable.get(dst)
          if (dstNbrs.nonEmpty) {
            val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
            computeMotifCount(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
          } else {
            Iterator.empty
          }
        }
      }

    }
  }

  def calMotifDirectedCountV2(psModel: NeighborTableModel): Iterator[((Long, Byte), Int)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = psNbrTable.get(src)

        srcNbrs.filter(_._1 > src).flatMap { case (dst, tag) =>
          val dstNbrs = psNbrTable.get(dst)
          if (dstNbrs.nonEmpty) {
            val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
            computeMotifCount(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
          } else {
            Iterator.empty
          }
        }
      }
    }
  }

  // mark the superNodes
  def calMotifDirectedCountV2_(psModel: NeighborTableModel, superNodeThreshold: Int = Int.MaxValue): Iterator[((Long, Byte), Int)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = psNbrTable.get(src)
        val srcNbrs2Calc = srcNbrs.filter(_._1 > src)
        if (srcNbrs2Calc.length > superNodeThreshold) {
          Iterator.single((src, 0.toByte), -1)
        } else {
          srcNbrs2Calc.flatMap { case (dst, tag) =>
            val dstNbrs = psNbrTable.get(dst)
            if (dstNbrs.nonEmpty) {
              val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
              computeMotifCount(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
            } else {
              Iterator.empty
            }
          }
        }
      }

    }
  }

  def calTriangleCommonMotifDirected(psModel: NeighborTableModel): Iterator[((Long, Long, Byte), Byte)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNbrTable = new Long2ObjectOpenHashMap[Array[(Long, Byte)]](batchSize)

      (from until to).foreach { pos =>
        numSrcNodes += 1
        val edgeWithAttrs = new ArrayBuffer[(Long, Byte)](batchSize)
        for (idx <- neighbors(pos).indices) {
          val dst = neighbors(pos)(idx)
          val attr = edgeAttrs(pos)(idx).asInstanceOf[Byte]
          edgeWithAttrs += ((dst, attr))
        }
        localNbrTable.put(srcIds(pos), edgeWithAttrs.toArray)
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
      }

      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = psModel.getAttrLongNeighborTable[Byte](pullNodes.toArray) //get neighborsTable from ps

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNbrTable.keySet().toLongArray

      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcNbrs = localNbrTable.get(src)
        srcNbrs.flatMap { case (dst, tag) =>
          val dstNbrs = localNbrTable.getOrDefault(dst, psNbrTable.get(dst))
          val MsgMap = new mutable.HashMap[Long, CounterTriangleDirected]()

          if (dstNbrs != null && dstNbrs.nonEmpty) {
            val commonNbrs = TriangleCountingDirected.intersect(srcNbrs, dstNbrs).toMap
            computeCommonTriangleMotif(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
          } else {
            Iterator.empty
          }
        }
      }
    }
  }
}

object NeighborTablePartition {

  def motifTriangle = motifMatrix.toMap

  def motifEdge = motifTwoEdge

  def twoEdge = twoEdgeMatrix

  def fromNeighborTableRDD[ED: ClassTag](data: RDD[NeighborTable[ED]],
                                         isDirected: Boolean = false): RDD[NeighborTablePartition[ED]] = {
    data.mapPartitionsWithIndex { case (partId, iter) =>
      val localSrcs = new ArrayBuffer[VertexId]
      val localNeighbors = new ArrayBuffer[Array[VertexId]]
      val localAttrs = new ArrayBuffer[Array[ED]]
      iter.foreach { item =>
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


  def computeTriangleMotif(src: Long, dst: Long, tag: Byte, commonNbrs: Map[Long, (Byte, Byte)]): Array[(Long, Byte)] = {
    val motifCount = new ArrayBuffer[(Long, Byte)]() // (node,motifType)
    //motif type of triangle: (src,dst)and common neighbors

    if (commonNbrs.size > 0) {
      for ((node, (src2nodeTag, dst2nodeTag)) <- commonNbrs) {
        //only count triangle-motif only the smallest edge
        if (commonNbrs.contains(node) && node > math.max(src, dst) && src < dst) {
          //judge the motif type of this triangle
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)

          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }

          src2nodeTag match {
            case 0 => srcOut += 1; nodeIn += 1
            case 1 => srcIn += 1; nodeOut += 1
            case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
          }

          dst2nodeTag match {
            case 0 => dstOut += 1; nodeIn += 1
            case 1 => dstIn += 1; nodeOut += 1
            case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
          }

          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = motifTriangle.get(degSeq).get

          val vArr = Array(src, dst, node)
          for (i <- motifTag.indices) {
            if (motifTag(i) != 0) {
              motifCount.append((vArr(i), motifTag(i)))
            }
          }
        }
      }
    }
    motifCount.toArray
  }


  def computeTwoEdgeMotifMatrix(src: Long, dst: Long, tag: Byte,
                                srcNbrs: Array[(Long, Byte)], dstNbrs: Array[(Long, Byte)],
                                commonNbrs: Map[Long, (Byte, Byte)]): Array[(Long, Long, Byte)] = {
    val motifCount = new ArrayBuffer[(Long, Long, Byte)]() // (node,motifType)

    // now count twoEdge-motif,first count (src,dst) and srcNbrs
    if (srcNbrs.length >= 2) {
      var i = 0
      while (i < srcNbrs.length) {
        val (node, src2node) = srcNbrs(i)
        // two-edge condition , only compute on smallest edge: if smallest in middle, min(dst,node)

        if (!commonNbrs.contains(srcNbrs(i)._1) && src < math.min(dst, node) && dst < node) {
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)

          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }
          src2node match {
            case 0 => srcOut += 1; nodeIn += 1
            case 1 => srcIn += 1; nodeOut += 1
            case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
          }
          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = twoEdge.get(degSeq).get
          println(src, dst, node, degSeq, motifTag)
          motifCount.append((src, dst, motifTag))
          motifCount.append((math.min(src, node), math.max(src, node), motifTag))
          motifCount.append((math.min(dst, node), math.max(dst, node), motifTag))
        }
        // non-closure condition, onley compute on smallest edge
        i += 1
      }
    }

    if (dstNbrs.length >= 2) {
      var i = 0
      while (i < dstNbrs.length && dstNbrs(i)._1 != src) {
        val (node, nodeTag) = dstNbrs(i)

        if (!commonNbrs.contains(dstNbrs(i)._1) && src < math.min(dst, node)) {
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)
          val dst2node = dstNbrs(i)._2
          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }
          dst2node match {
            case 0 => if (dst < node) {
              dstOut += 1
              nodeIn += 1
            } else {
              dstIn += 1
              nodeOut += 1
            }
            case 1 => if (dst < node) {
              dstIn += 1
              nodeOut += 1
            } else {
              dstOut += 1
              nodeIn += 1
            }
            case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
          }
          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = twoEdge.get(degSeq).get
          println(src, dst, node, degSeq, motifTag)
          motifCount.append((src, dst, motifTag))
          motifCount.append((math.min(src, node), math.max(src, node), motifTag))
          motifCount.append((math.min(dst, node), math.max(dst, node), motifTag))
        }
        i += 1
      }
    }

    motifCount.toArray
  }

  /**
    *
    * @param src        src node
    * @param dst        dst node
    * @param tag        direction tag: 0,1,2...
    * @param srcNbrs    neighbors of srcNode
    * @param dstNbrs    neighbors of dstNode
    * @param commonNbrs common neighbors of srcNode and dstNode
    * @return
    */
  def computeMotif(src: Long, dst: Long, tag: Byte,
                   srcNbrs: Array[(Long, Byte)], dstNbrs: Array[(Long, Byte)],
                   commonNbrs: Map[Long, (Byte, Byte)]): Array[(Long, Byte)] = {
    val motifCount = new ArrayBuffer[(Long, Byte)]() // (node,motifType)

    //motif type of this edge
    val (edgeMotifSrc, edgeMotifDst) = if (tag == 0) (1, 2) else if (tag == 1) (2, 1) else (14, 14)
    motifCount.append((src, edgeMotifSrc.toByte))
    motifCount.append((dst, edgeMotifDst.toByte))

    //motif type of triangle: (src,dst)and common neighbors

    if (commonNbrs.size > 0) {
      for ((node, (src2nodeTag, dst2nodeTag)) <- commonNbrs) {
        var (srcIn, srcOut) = (0, 0)
        var (dstIn, dstOut) = (0, 0)
        var (nodeIn, nodeOut) = (0, 0)
        //only count triangle-motif only the smallest edge
        if (node > math.max(src, dst) && src < dst) {
          //judge the motif type of this triangle
          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }

          src2nodeTag match {
            case 0 => srcOut += 1; nodeIn += 1
            case 1 => srcIn += 1; nodeOut += 1
            case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
          }

          dst2nodeTag match {
            case 0 => dstOut += 1; nodeIn += 1
            case 1 => dstIn += 1; nodeOut += 1
            case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
          }

          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = motifTriangle.get(degSeq).get
          //          println(src, dst, node, degSeq,motifTag.mkString(","))

          val vArr = Array(src, dst, node)
          for (t <- motifTag.indices) {
            if (motifTag(t) != 0) {
              motifCount.append((vArr(t), motifTag(t)))
            }
          }
        }
      } // triangle-motif has been count
    }

    // now count twoEdge-motif,first count (src,dst) and srcNbrs
    if (srcNbrs.length >= 1) {
      var i = 0
      while (i < srcNbrs.length) {
        val (node, src2node) = srcNbrs(i)
        // two-edge condition , only compute on smallest edge: if smallest in middle, min(dst,node)

        if (!commonNbrs.contains(srcNbrs(i)._1) && src < math.min(dst, node) && dst < node) {
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)

          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }
          src2node match {
            case 0 => srcOut += 1; nodeIn += 1
            case 1 => srcIn += 1; nodeOut += 1
            case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
          }
          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = motifTwoEdge.get(degSeq).get
          //          println(src, dst, node, degSeq,motifTag.mkString(","))

          val vArr = Array(src, dst, node)
          for (t <- motifTag.indices) {
            if (motifTag(t) != 0) {
              motifCount.append((vArr(t), motifTag(t)))
            }
          }
        }
        // non-closure condition, onley compute on smallest edge
        i += 1
      }
    }

    if (dstNbrs.length >= 1) {
      var i = 0
      while (i < dstNbrs.length) {
        val (node, nodeTag) = dstNbrs(i)

        if (!commonNbrs.contains(dstNbrs(i)._1) && src < math.min(dst, node)) {
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)
          val dst2node = dstNbrs(i)._2
          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }
          dst2node match {
            case 0 => if (dst < node) {
              dstOut += 1
              nodeIn += 1
            } else {
              dstIn += 1
              nodeOut += 1
            }
            case 1 => if (dst < node) {
              dstIn += 1
              nodeOut += 1
            } else {
              dstOut += 1
              nodeIn += 1
            }
            case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
          }
          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = motifTwoEdge.get(degSeq).get
          //          println(src, dst, node, degSeq,motifTag.mkString(","))

          val vArr = Array(src, dst, node)
          //motifTag 221111 need to handle todo
          for (t <- motifTag.indices) {
            if (motifTag(t) != 0) {
              motifCount.append((vArr(t), motifTag(t)))
            }
          }
        }
        i += 1
      }
    }
    motifCount.toArray

  }


  def computeMotifCount(src: Long, dst: Long, tag: Byte,
                        srcNbrs: Array[(Long, Byte)], dstNbrs: Array[(Long, Byte)],
                        commonNbrs: Map[Long, (Byte, Byte)]): Array[((Long, Byte), Int)] = {
    val motifCount = new ArrayBuffer[((Long, Byte), Int)]() // (node,motifType)

    //motif type of this edge
    val (edgeMotifSrc, edgeMotifDst) = if (tag == 0) (1, 2) else if (tag == 1) (2, 1) else (14, 14)
    motifCount.append(((src, edgeMotifSrc.toByte), 1))
    motifCount.append(((dst, edgeMotifDst.toByte), 1))

    //motif type of triangle: (src,dst)and common neighbors

    if (commonNbrs.size > 0) {
      for ((node, (src2nodeTag, dst2nodeTag)) <- commonNbrs) {
        var (srcIn, srcOut) = (0, 0)
        var (dstIn, dstOut) = (0, 0)
        var (nodeIn, nodeOut) = (0, 0)
        //only count triangle-motif only the smallest edge
        if (node > math.max(src, dst) && src < dst) {
          //judge the motif type of this triangle
          tag match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }

          src2nodeTag match {
            case 0 => srcOut += 1; nodeIn += 1
            case 1 => srcIn += 1; nodeOut += 1
            case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
          }

          dst2nodeTag match {
            case 0 => dstOut += 1; nodeIn += 1
            case 1 => dstIn += 1; nodeOut += 1
            case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
          }

          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = motifTriangle.get(degSeq).get
          //          println(src, dst, node, degSeq,motifTag.mkString(","))

          val vArr = Array(src, dst, node)
          for (t <- motifTag.indices) {
            if (motifTag(t) != 0) {
              motifCount.append(((vArr(t), motifTag(t)), 1))
            }
          }
        }
      } // triangle-motif has been count
    }

    // now count twoEdge-motif,first count (src,dst) and srcNbrs
    val r1 = CheckMotif.srcNbrsMotif(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)
    val r2 = CheckMotif.dstNbrsMotif(src, dst, tag, srcNbrs, dstNbrs, commonNbrs)

    if (r1.length > 0)
      motifCount.appendAll(r1)
    if (r2.length > 0)
      motifCount.appendAll(r2)
    motifCount.toArray
  }


  def computeCommonTriangleMotif(src: Long, dst: Long, tag: Byte,
                                 srcNbrs: Array[(Long, Byte)],
                                 dstNbrs: Array[(Long, Byte)],
                                 commonNbrs: Map[Long, (Byte, Byte)]): Array[((Long, Long, Byte), Byte)] = {
    val motifCount = new ArrayBuffer[((Long, Long, Byte), Byte)]()

    if (commonNbrs.nonEmpty) {
      for ((node, (src2nodeTag, dst2nodeTag)) <- commonNbrs) {
        //only count triangle-motif only the smallest edge
        if (commonNbrs.contains(node) && node > math.max(src, dst) && src < dst) {
          //judge the motif type of this triangle
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)

          tag match {
            case 0 => srcOut += 1;
              dstIn += 1
            case 1 => srcIn += 1;
              dstOut += 1
            case 2 => srcOut += 1;
              dstIn += 1;
              srcIn += 1;
              dstOut += 1
          }

          src2nodeTag match {
            case 0 => srcOut += 1;
              nodeIn += 1
            case 1 => srcIn += 1;
              nodeOut += 1
            case 2 => srcOut += 1;
              nodeIn += 1;
              srcIn += 1;
              nodeOut += 1
          }

          dst2nodeTag match {
            case 0 => dstOut += 1;
              nodeIn += 1
            case 1 => dstIn += 1;
              nodeOut += 1
            case 2 => dstOut += 1;
              nodeIn += 1;
              dstIn += 1;
              nodeOut += 1
          }

          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = commonTriangleMotifMatrix.get(degSeq).get
          //          println(src, dst, node, degSeq,motifTag.mkString(","))

          motifCount.append(((src, dst, tag), motifTag))
          motifCount.append(((src, node, src2nodeTag), motifTag))
          motifCount.append(((dst, node, dst2nodeTag), motifTag))
        }
      } // triangle-motif has been count
    }
    motifCount.toArray
  }

  def commonTriangleMotifMatrix: Map[String, Byte] = {
    val matrix = collection.mutable.Map[String, Byte]()
    matrix.put("111111", 9)
    matrix.put("200211", 10)
    matrix.put("201102", 10)
    matrix.put("022011", 10)
    matrix.put("021120", 10)
    matrix.put("112002", 10)
    matrix.put("110220", 10)
    matrix.put("222222", 13)

    matrix.put("022121", 15)
    matrix.put("210221", 15)
    matrix.put("212102", 15)

    matrix.put("201212", 14)
    matrix.put("122012", 14)
    matrix.put("121220", 14)

    matrix.put("121121", 11)
    matrix.put("122111", 11)
    matrix.put("111221", 11)
    matrix.put("112112", 11)
    matrix.put("211112", 11)
    matrix.put("211211", 11)

    matrix.put("122221", 12)
    matrix.put("122122", 12)
    matrix.put("221221", 12)
    matrix.put("222112", 12)
    matrix.put("212212", 12)
    matrix.put("211222", 12)

    matrix.toMap
  }

  def motifMatrix = {
    val matrix = collection.mutable.Map[String, Array[Byte]]()
    matrix.put("200211", Array(11, 12, 13))
    matrix.put("201102", Array(11, 13, 12))
    matrix.put("022011", Array(12, 11, 13))
    matrix.put("021120", Array(12, 13, 11))
    matrix.put("112002", Array(13, 11, 12))
    matrix.put("110220", Array(13, 12, 11))

    matrix.put("111111", Array(10, 10, 10))

    matrix.put("222222", Array(33, 33, 33))

    matrix.put("022121", Array(23, 24, 24))
    matrix.put("210221", Array(24, 23, 24))
    matrix.put("212102", Array(24, 24, 23))

    matrix.put("201212", Array(25, 26, 26))
    matrix.put("122012", Array(26, 25, 26))
    matrix.put("121220", Array(26, 26, 25))

    matrix.put("121121", Array(27, 28, 29))
    matrix.put("122111", Array(27, 29, 28))
    matrix.put("111221", Array(28, 27, 29))
    matrix.put("112112", Array(28, 29, 27))
    matrix.put("211112", Array(29, 28, 27))
    matrix.put("211211", Array(29, 27, 28))

    matrix.put("122221", Array(30, 31, 32))
    matrix.put("122122", Array(30, 32, 31))
    matrix.put("221221", Array(31, 30, 32))
    matrix.put("222112", Array(31, 32, 30))
    matrix.put("212212", Array(32, 31, 30))
    matrix.put("211222", Array(32, 30, 31))

    matrix.put("222222", Array(33, 33, 33))
    matrix.toMap
  }


  def motifTwoEdge = {
    val matrix = collection.mutable.Map[String, Array[Byte]]()

    matrix.put("011011", Array(3, 5, 4))
    matrix.put("011110", Array(3, 4, 5))
    matrix.put("110110", Array(4, 3, 5))
    matrix.put("111001", Array(4, 5, 3))
    matrix.put("101101", Array(5, 4, 3))
    matrix.put("100111", Array(5, 3, 4))

    matrix.put("010120", Array(7, 7, 6))
    matrix.put("012001", Array(7, 6, 7))
    matrix.put("200101", Array(6, 7, 7))

    matrix.put("101002", Array(9, 9, 8))
    matrix.put("100210", Array(9, 8, 9))
    matrix.put("021010", Array(8, 9, 9))

    matrix.put("111210", Array(15, 16, 17))
    matrix.put("111012", Array(15, 17, 16))
    matrix.put("101112", Array(17, 15, 16))
    matrix.put("101211", Array(17, 16, 15))
    matrix.put("121011", Array(16, 17, 15))
    matrix.put("121110", Array(16, 15, 17))

    matrix.put("110121", Array(18, 20, 19))
    matrix.put("112101", Array(18, 19, 20))
    matrix.put("211101", Array(19, 18, 20))
    matrix.put("210111", Array(19, 20, 18))
    matrix.put("012111", Array(20, 19, 18))
    matrix.put("011121", Array(20, 18, 19))
    matrix.put("221111", Array(21, 22, 22))
    matrix.put("112211", Array(22, 21, 22))
    matrix.put("111122", Array(22, 22, 21))
    matrix.toMap
  }

  def twoEdgeMatrix = {
    val matrix = collection.mutable.Map[String, Byte]()

    matrix.put("011011", 3)
    matrix.put("011110", 3)
    matrix.put("110110", 3)
    matrix.put("111001", 3)
    matrix.put("101101", 3)
    matrix.put("100111", 3)

    matrix.put("010120", 5)
    matrix.put("012001", 5)
    matrix.put("200101", 5)

    matrix.put("101002", 4)
    matrix.put("100210", 4)
    matrix.put("021010", 4)

    matrix.put("111210", 6)
    matrix.put("111012", 6)
    matrix.put("101112", 6)
    matrix.put("101211", 6)
    matrix.put("121011", 6)
    matrix.put("121110", 6)

    matrix.put("110121", 7)
    matrix.put("112101", 7)
    matrix.put("211101", 7)
    matrix.put("210111", 7)
    matrix.put("012111", 7)
    matrix.put("011121", 7)

    matrix.put("221111", 8)
    matrix.put("112211", 8)
    matrix.put("111122", 8)
    matrix.toMap
  }
}
