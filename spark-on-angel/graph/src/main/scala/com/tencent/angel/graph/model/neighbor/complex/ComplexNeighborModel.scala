package com.tencent.angel.graph.model.neighbor.complex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetElementResult
import com.tencent.angel.graph.model.general.get.GeneralGet
import com.tencent.angel.graph.model.general.init.GeneralInit
import com.tencent.angel.graph.model.ops.CommonOps
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class ComplexNeighborModel(modelContex: ModelContext) extends ComplexNeighborOps with CommonOps with Serializable {
  var neighborMatrix: PSMatrix = _

  override def init(mc: MatrixContext): Unit = {
    val mc = ModelContextUtils.createMatrixContext(
      modelContex, RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsAttrTagElement])
    mc.setParts(mc.getParts)
    neighborMatrix = PSMatrix.matrix(mc)
  }

  override def checkpoint(): Unit = neighborMatrix.checkpoint(0)

  override def initNeighbors(nodeIds: Array[Long], neighbors: Array[IElement]): Unit = {
    neighborMatrix.psfUpdate(new GeneralInit(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, neighbors))).get()
  }

  override def initNeighbors(neighborTable: Seq[(Long, IElement)]): Unit = {
    val nodeIds = new Array[Long](neighborTable.length)
    val neighbors = new Array[IElement](neighborTable.length)

    neighborTable.zipWithIndex.foreach(e => {
      nodeIds(e._2) = e._1._1
      neighbors(e._2) = e._1._2
    })

    initNeighbors(nodeIds, neighbors)
  }

  override def getNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrTagElement] = {
    neighborMatrix.psfGet(
      new GeneralGet(
        new LongKeysGetParam(neighborMatrix.id, nodeIds)))
      .asInstanceOf[GetElementResult].getData.asInstanceOf[Long2ObjectOpenHashMap[NeighborsAttrTagElement]]
  }

  override def sampleNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrTagElement] = {
    //TODO
    new Long2ObjectOpenHashMap[NeighborsAttrTagElement]()
  }
}
