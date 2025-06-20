package com.tencent.angel.graph.kclique


import com.tencent.angel.graph.data.{PartitionId, VertexId}
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.utils.element.{GraphStats, NeighborTable}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class KCliqueGraphPartition[@specialized(
  Byte, Boolean, Short, Int, Long, Float, Double) ED: ClassTag](isDirected: Boolean,
                                                                var partitionID: PartitionId,
                                                                var srcIds: Array[VertexId],
                                                                var neighbors: Array[Array[VertexId]],
                                                                var edgeAttrs: Array[Array[ED]],
                                                                var srcInCliques: Array[Array[VertexId]]) extends Serializable {
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

  def takeBatch(from: Int, length: Int): Array[NeighborTable[ED]] = {
    val length2 = length min (size - from)
    val rec = new Array[NeighborTable[ED]](length2)
    (0 until length2).foreach { pos =>
      rec(pos) = NeighborTable(srcIds(from + pos),
        neighbors(from + pos), null,
        if (edgeAttrs == null) null else edgeAttrs(from + pos))
    }
    rec
  }

  def batchIterator(batchSize: Int): Iterator[Array[NeighborTable[ED]]] =
    BatchIter(iterator, batchSize)

  def iterator: Iterator[NeighborTable[ED]] = new Iterator[NeighborTable[ED]] {
    private[this] val instance = new NeighborTable[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < KCliqueGraphPartition.this.size

    override def next(): NeighborTable[ED] = {
      instance.srcId = srcIds(pos)
      instance.neighborIds = neighbors(pos)
      instance.attrs = if (edgeAttrs == null || edgeAttrs.isEmpty) null else edgeAttrs(pos)
      pos += 1
      instance
    }
  }

  def testPS(psModel: KCliquePSModel, num: Int): Boolean = {
    var correct = true
    assert(num <= size, s"the size of partition $size < $num")
    // check neighbor table
    for (i <- 0 until num - 1) {
      val srcId1 = srcIds(i)
      val srcId2 = srcIds(i + 1)
      val clk1 = neighbors(i).filter(_ > srcId1)
      val clk2 = neighbors(i + 1).filter(_ > srcId2)
      val trueNum = ArrayUtils.unionCount(clk1, clk2)
      val fromPS = psModel.readMsgs(Array(srcId1, srcId2))
      val psNum = ArrayUtils.unionCount(fromPS.get(srcId1), fromPS.get(srcId2))
      println(s"2-clique of $srcId1 = ${clk1.length} [RDD] ${fromPS.get(srcId1).length} [PS], " +
        s"2-clique of $srcId2 = ${clk2.length} [RDD] ${fromPS.get(srcId2).length} [PS], " +
        s"common friends = $trueNum [RDD] $psNum [PS]")
      if (correct && trueNum != psNum)
        correct = false
    }
    correct
  }

  // calculate kclique, return num of cliques
  def calculateKClique(psModel: KCliquePSModel, nK: Int): Long = {

    println(s"${nK}-clique calculation process start...")
    val batchSize = psModel.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    val startTs = System.currentTimeMillis()
    var cliqueNum = 0L

    makeBatchIterator(batchSize).foreach { case (from, to) =>
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
      val psCliqueTable = psModel.readMsgs(pullNodes.toArray)

      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size

      println(s"partition $partitionID: process $numSrcNodes neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray
      val newCliqueTable = ArrayBuffer[NeighborTable[ED]]()

      srcNodes.foreach { src =>
        val srcNewClique = ArrayBuffer[Long]()
        // odd-cliques are stored in maximal nodes, even-clique in minimal nodes
        val srcNeighbors = if (nK % 2 == 1) localNeighborTable.get(src).filter(_ < src) else localNeighborTable.get(src).filter(_ > src)
        val srcNbrSet = srcNeighbors.toSet

        srcNeighbors.foreach { dst =>

          // every (nK-2) nodes combined with dst makes a clique
          val dstCliques = psCliqueTable.get(dst)
          require(dstCliques.length % (nK - 2) == 0, s"dst cliques should be divided by (nK-2)")
          val numCliques = dstCliques.length / (nK - 2)
          // estimate whether all nodes in dst cliques are subset of srcNbrs
          (0 until numCliques).foreach { kCq =>
            val dCliqueNodes = collection.mutable.Set[Long]()
            dCliqueNodes.add(dst)
            val cStart = kCq * (nK - 2)
            val cEnd = (kCq + 1) * (nK - 2)
            (cStart until cEnd).foreach { idx =>
              dCliqueNodes.add(dstCliques(idx))
            }
            require(dCliqueNodes.size == (nK - 1), s"something went wrong causing error in clique nodes collection")
            // if dCliqueNodes subset of srcNbrSet, a nK-Clique is found
            if (dCliqueNodes.subsetOf(srcNbrSet)) {
              srcNewClique ++= dCliqueNodes.toArray
              cliqueNum += 1
            }
            dCliqueNodes.clear()
          }
        }
        newCliqueTable += NeighborTable(src, srcNewClique.toArray)
      }
      psModel.writeMsgs(newCliqueTable.toArray)
    }
    cliqueNum
  }

  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, KCliqueGraphPartition.this.size))
    }

    override def hasNext: Boolean = {
      index < KCliqueGraphPartition.this.size
    }
  }

  def countNodes(psModel: KCliquePSModel): Long = {
    val kCliques = psModel.readMsgs(srcIds)
    val keyArray = kCliques.keySet().toLongArray
    var cnt = 0L
    keyArray.foreach { key =>
      println(s"nodeId: $key contains: ${kCliques.get(key).mkString(",")}")
      if (kCliques.get(key).length > 0) {
        cnt += 1
      }
    }
    cnt
  }

  def save(psModel: KCliquePSModel, kC: Int): Array[Array[Long]] = {
    val kCliques = psModel.readMsgs(srcIds)
    val keyArray = kCliques.keySet().toLongArray
    val retCliques = ArrayBuffer[Array[Long]]()
    keyArray.foreach { key =>
      val kClen = kCliques.get(key).length
      if (kClen > 0) {
        for (k <- 0 until (kClen / (kC - 1))) {
          val tClique = ArrayBuffer[Long](key)
          for (i <- 0 until (kC - 1)) {
            tClique += kCliques.get(key)(k * (kC - 1) + i)
          }
          retCliques += tClique.toArray
        }
      }
    }
    retCliques.toArray
  }

  private def this(isDirected: Boolean) = this(isDirected, -1, null, null, null, null)

  private def this() = this(false)
}

object KCliqueGraphPartition {
  def fromNeighborTableRDD[ED: ClassTag](data: RDD[NeighborTable[ED]],
                                         isDirected: Boolean = false): RDD[KCliqueGraphPartition[ED]] = {
    data.mapPartitionsWithIndex { case (partId, iter) =>
      val localSrcs = new ArrayBuffer[VertexId]
      val localNeighbors = new ArrayBuffer[Array[VertexId]]
      val localAttrs = new ArrayBuffer[Array[ED]]
      val localSrcCliques = new ArrayBuffer[Array[VertexId]]()
      iter.foreach { item =>
        localSrcs += item.srcId
        localNeighbors += item.neighborIds
        localSrcCliques += item.neighborIds.filter(_ > item.srcId)
        if (item.attrs != null)
          localAttrs += item.attrs
      }
      Iterator.single(
        new KCliqueGraphPartition[ED](
          isDirected,
          partId,
          localSrcs.toArray,
          localNeighbors.toArray,
          if (localAttrs.isEmpty) null else localAttrs.toArray,
          localSrcCliques.toArray
        )
      )
    }
  }

  def getStats[ED: ClassTag](data: RDD[KCliqueGraphPartition[ED]]): GraphStats = {
    data.map(_.stats).reduce(_ + _)
  }
}

