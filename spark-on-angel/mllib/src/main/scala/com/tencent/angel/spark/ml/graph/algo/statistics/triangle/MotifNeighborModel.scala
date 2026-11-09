package com.tencent.angel.spark.ml.graph.algo.statistics.triangle

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.neighbor.complex.{ComplexNeighborModel, NeighborsAttrTagElement}
import com.tencent.angel.graph.model.neighbor.simple.psf.get.GetLongNeighbor
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.spark.ml.graph.algo.embedding.line2.CheckpointUtils
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import scala.collection.mutable.ArrayBuffer

class MotifNeighborModel(neighborContext: ModelContext, tagContext: ModelContext = null)
  extends ComplexNeighborModel(neighborContext) {
  var tagMatrix: PSMatrix = _

  override def init(): Unit = {
    val neighborMc = ModelContextUtils.createMatrixContext(
      neighborContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsAttrTagElement])
    neighborMc.setParts(neighborMc.getParts)
    neighborMatrix = PSMatrix.matrix(neighborMc)

    if (tagContext != null) {
      val tagMc = ModelContextUtils.createMatrixContext(
        tagContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[LongArrayElement])
      tagMc.setParts(tagMc.getParts)
      tagMatrix = PSMatrix.matrix(tagMc)
    }
  }

  override def checkpoint(): Unit = {
    val matrixNames = new ArrayBuffer[String]
    matrixNames.append(neighborMatrix.name)
    if (tagContext != null) matrixNames.append(tagMatrix.name)
    CheckpointUtils.checkpoint(0, matrixNames.toArray)
  }

  def updateTag(iter: Iterator[(Long, Array[Long])], batchSize: Int): Unit = {
    iter.sliding(batchSize, batchSize).foreach { batch =>
      val nodeIds = new Array[Long](batch.size)
      val neighborElems = new Array[IElement](batch.size)
      batch.zipWithIndex.foreach(e => {
        nodeIds(e._2) = e._1._1
        neighborElems(e._2) = new LongArrayElement(e._1._2)
      })
      tagMatrix.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(tagMatrix.id, nodeIds, neighborElems))).get()
    }
  }

  def readTag(nodes: Array[Long]): Long2ObjectOpenHashMap[Set[Long]] = {
    val temp = tagMatrix.psfGet(new GetLongNeighbor(
      new LongKeysGetParam(tagMatrix.id, nodes))).asInstanceOf[GetLongsResult].getData
    val re = new Long2ObjectOpenHashMap[Set[Long]](temp.size())
    nodes.foreach { node => re.put(node, temp.get(node).toSet)}
    temp.clear()
    re
  }

  def clearNeighbors(): Unit = {
    neighborMatrix.reset()
  }
}