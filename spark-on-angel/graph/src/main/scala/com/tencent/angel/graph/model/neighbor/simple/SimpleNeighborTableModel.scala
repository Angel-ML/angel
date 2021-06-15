package com.tencent.angel.graph.model.neighbor.simple

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.neighbor.simple.psf.get.{GetByteNeighbor, GetLongNeighbor}
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.model.ops.CommonOps
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.{ByteArrayElement, IElement, LongArrayElement}
import com.tencent.angel.spark.models.PSMatrix
import com.twitter.chill.ScalaKryoInstantiator
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class SimpleNeighborTableModel(modelContex: ModelContext) extends NeighborOps with CommonOps with Serializable {

  var neighborMatrix: PSMatrix = _

  override def initNeighbors(nodeIds: Array[Long], neighbors: Array[Array[Long]]): Unit = {
    var neighborElems: Array[IElement] = null
    if (modelContex.isUseBytesFormatForReadOnly) {
      neighborElems = neighbors.map(
        e => new ByteArrayElement(ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(e)).asInstanceOf[IElement])
    } else {
      neighborElems = neighbors.map(e => new LongArrayElement(e))
    }
    neighborMatrix.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, neighborElems))).get()
  }

  override def initNeighbors(neighborTable: Seq[(Long, Array[Long])]): Unit = {
    val nodeIds = new Array[Long](neighborTable.size)
    val neighborElems = new Array[IElement](neighborTable.size)
    if (modelContex.isUseBytesFormatForReadOnly) {
      neighborTable.zipWithIndex.foreach(e => {
        nodeIds(e._2) = e._1._1
        neighborElems(e._2) = new ByteArrayElement(ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(e._1._2))
      })
    } else {
      neighborTable.zipWithIndex.foreach(e => {
        nodeIds(e._2) = e._1._1
        neighborElems(e._2) = new LongArrayElement(e._1._2)
      })
    }
    neighborMatrix.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, neighborElems))).get()
  }

  override def getNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    if (modelContex.isUseBytesFormatForReadOnly) {
      neighborMatrix.psfGet(
        new GetByteNeighbor(
          new LongKeysGetParam(neighborMatrix.id, nodeIds))).asInstanceOf[GetLongsResult].getData
    } else {
      neighborMatrix.psfGet(
        new GetLongNeighbor(
          new LongKeysGetParam(neighborMatrix.id, nodeIds))).asInstanceOf[GetLongsResult].getData
    }
  }

  override def sampleNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    throw new UnsupportedOperationException("")
  }

  override def init(mc: MatrixContext): Unit = {
    val valueClass = if (modelContex.isUseBytesFormatForReadOnly) {
      classOf[ByteArrayElement]
    } else {
      classOf[LongArrayElement]
    }

    val mc = ModelContextUtils.createMatrixContext(modelContex, RowType.T_ANY_LONGKEY_SPARSE, valueClass)
    neighborMatrix = PSMatrix.matrix(mc)
  }

  override def checkpoint(): Unit = {
    neighborMatrix.checkpoint(0)
  }
}
