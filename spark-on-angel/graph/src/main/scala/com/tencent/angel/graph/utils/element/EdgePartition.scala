package com.tencent.angel.graph.utils.element

import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.utils.element.Element.{PartitionId, VertexId, VertexSet}
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class EdgePartition[@specialized(
  Byte, Boolean, Short, Int, Long, Float, Double) ED: ClassTag](isDirected: Boolean,
                                                                partitionID: PartitionId,
                                                                var srcIds: Array[VertexId],
                                                                var dstIds: Array[VertexId],
                                                                var edgeAttrs: Array[ED]) extends Serializable {

  private def this(isDirected: Boolean) = this(isDirected, -1, null, null, null)

  private def this() = this(false, -1, null, null, null)

  def fromEdgeRDD(data: Iterator[Edge[ED]]): this.type = {
    val localSrcs = new ArrayBuffer[VertexId]
    val localDsts = new ArrayBuffer[VertexId]
    val localAttrs = new ArrayBuffer[ED]
    data.foreach { edge =>
      localSrcs += edge.srcId
      localDsts += edge.dstId
      localAttrs += edge.attr
    }
    srcIds = localSrcs.toArray
    dstIds = localDsts.toArray
    edgeAttrs = localAttrs.toArray
    this
  }

  lazy val numEdges: Long = srcIds.length.toLong

  lazy val numVertices: Long = {
    val vertexSet = new VertexSet
    srcIds.foreach(vertexSet.add)
    dstIds.foreach(vertexSet.add)
    vertexSet.size()
  }

  lazy val stats: GraphStats = {
    var minVertex = Long.MaxValue
    var maxVertex = Long.MinValue
    var numEdges = 0L
    (0 until numEdges.toInt).foreach { pos =>
      minVertex = minVertex min srcIds(pos) min dstIds(pos)
      maxVertex = maxVertex max srcIds(pos) max dstIds(pos)
      numEdges += 1
    }
    GraphStats(minVertex, maxVertex, -1, numEdges)
  }

  def iterator: Iterator[Edge[ED]] = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.numEdges

    override def next(): Edge[ED] = {
      edge.srcId = srcIds(pos)
      edge.dstId = dstIds(pos)
      edge.attr = edgeAttrs(pos)
      pos += 1
      edge
    }
  }

  def batchIterator(batchSize: Int): Iterator[Array[Edge[ED]]] =
    BatchIter(iterator, batchSize)
}

object EdgePartition {

  def fromEdgeRDD[ED: ClassTag](data: RDD[Edge[ED]],
                                isDirected: Boolean = false): RDD[EdgePartition[ED]] = {
    data.mapPartitionsWithIndex { case (partId, iter) =>
      val localSrcs = new ArrayBuffer[VertexId]
      val localDsts = new ArrayBuffer[VertexId]
      val localAttrs = new ArrayBuffer[ED]
      iter.map { edge =>
        localSrcs += edge.srcId
        localDsts += edge.dstId
        localAttrs += edge.attr
      }
      Iterator.single(
        new EdgePartition[ED](
          isDirected,
          partId,
          localSrcs.toArray,
          localDsts.toArray,
          localAttrs.toArray))
    }
  }
}
