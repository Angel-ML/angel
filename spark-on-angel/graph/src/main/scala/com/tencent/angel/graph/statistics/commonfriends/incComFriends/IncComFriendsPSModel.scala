package com.tencent.angel.graph.statistics.commonfriends.incComFriends

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.graph.model.neighbor.dynamic.psf.init.InitDynamicNbrs
import com.tencent.angel.graph.model.neighbor.dynamic.{DynamicNeighborElement, DynamicNeighborModel}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class IncComFriendsPSModel(nbrModelContext: ModelContext,
                           tagModelContext: ModelContext)
  extends DynamicNeighborModel(nbrModelContext) {

  var tagMatrix: PSMatrix = _

  override def init(): Unit = {
    val nbrMc = ModelContextUtils.createMatrixContext(
      nbrModelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[DynamicNeighborElement])
    nbrMc.setParts(nbrMc.getParts)
    neighborMatrix = PSMatrix.matrix(nbrMc)

    val tagMc = ModelContextUtils.createMatrixContext(tagModelContext, RowType.T_INT_SPARSE_LONGKEY)
    tagMc.setParts(tagMc.getParts)
    tagMatrix = PSMatrix.matrix(tagMc)
  }

  override def checkpoint(): Unit = {
    neighborMatrix.checkpoint()
    tagMatrix.checkpoint()
  }

  def initNeighborsWithInfo(data: RDD[(Long, Long, Int)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => initNeighborsWithInfo(pairs) }
    }.reduce(_ + _)
  }

  def initNeighborsWithInfo(pairs: Seq[(Long, Long, Int)]): Long = {
    var startTime = System.currentTimeMillis()
    val aggreResult = new Long2ObjectOpenHashMap[ArrayBuffer[Long]]()
    pairs.foreach { case (src, dst, _) =>
      var neighbors: ArrayBuffer[Long] = aggreResult.get(src)
      if (neighbors == null) {
        neighbors = new ArrayBuffer[Long]()
        neighbors.append(dst)
        aggreResult.put(src, neighbors)
      } else
        neighbors.append(dst)
    }
    val nodeIds = aggreResult.keySet().toLongArray()
    val neighbors = nodeIds.map(x => new LongArrayElement(aggreResult.get(x).toArray).asInstanceOf[IElement])
    aggreResult.clear()
    val aggreTime = System.currentTimeMillis() - startTime

    startTime = System.currentTimeMillis()
    val func = new InitDynamicNbrs(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, neighbors))
    neighborMatrix.psfUpdate(func).get()
    val pushTime = System.currentTimeMillis() - startTime
    println(s"init ${pairs.length} edges (${nodeIds.length} nodes with neighbors), processTime=$aggreTime, pushTime=$pushTime, ")
    aggreResult.clear()
    pairs.length.toLong
  }

  def updateTag(nodes: RDD[Long]): Double = {
    nodes.mapPartitions { iter =>
      Iterator.single(updateTag(iter.toArray))
    }.sum()
  }

  def updateTag(nodes: Array[Long]): Int = {
    val len = nodes.length
    val msgs = VFactory.sparseLongKeyIntVector(len, nodes, Array.fill(len)(1))
    tagMatrix.update(msgs)
    println(s"init ${nodes.length} taggedNodes.")
    nodes.length
  }

  def readTag(nodes: Array[Long]): LongIntVector = {
    tagMatrix.pull(0, nodes).asInstanceOf[LongIntVector]
  }

}