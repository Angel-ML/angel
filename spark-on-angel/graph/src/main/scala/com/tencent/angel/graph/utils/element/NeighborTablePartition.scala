package com.tencent.angel.graph.utils.element

import com.tencent.angel.graph.NeighborTableModel
import com.tencent.angel.graph.utils.element.Element._
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.spark.ml.util.ArrayUtils.intersect
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
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

  def generateLongMsg(hashMap: mutable.HashMap[VertexId, Long], msg: (VertexId, Long)): Unit = {
    if (hashMap.contains(msg._1)) {
      val count = hashMap(msg._1)
      hashMap.put(msg._1, count + msg._2)
    } else {
      hashMap.put(msg._1, msg._2)
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
            val commonFriends = intersect[ED](srcNeighbors, dstNeighbors)
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

  def intersections(arr1: Array[Long], arr2: Array[Long]): Array[Long] = {
    val re = new ArrayBuffer[Long]()
    var i = 0
    var j = 0
    while (i < arr1.length && j < arr2.length) {
      if (arr1(i) < arr2(j))
        i += 1
      else if (arr1(i) > arr2(j))
        j += 1
      else {
        re += arr1(i)
        i += 1
        j += 1
      }
    }
    re.toArray
  }

  def calTriangleUndirected(psModel: NeighborTableModel, computeLCC: Boolean): Iterator[(VertexId, Int, Float)] = {
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()

    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      println(s"partition $partitionID: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
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
        val srcNeighbors = localNeighborTable.get(src)
        var triangleCount = 0
        srcNeighbors.foreach { dst =>
          val dstNeighbors = if (localNeighborTable.containsKey(dst)) localNeighborTable.get(dst)
          else psNeighborsTable.get(dst)
          val comFriends = ArrayUtils.intersectCount(dstNeighbors, srcNeighbors)
          triangleCount += comFriends
        }
        if (computeLCC) {
          val numCompleteEdges = if (srcNeighbors.length > 1) srcNeighbors.length * (srcNeighbors.length - 1) / 2 else 0
          val lcc = if (numCompleteEdges == 0) 0f else triangleCount.toFloat / 2 / numCompleteEdges
          Iterator.single((src, triangleCount / 2, lcc))
        }
        else
          Iterator.single((src, triangleCount / 2, 0f))
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
}
