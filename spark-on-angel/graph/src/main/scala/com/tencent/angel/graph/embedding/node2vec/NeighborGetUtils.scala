package com.tencent.angel.graph.embedding.node2vec

import com.tencent.angel.exception.AngelException
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base.{GeneralPartGetParam, PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp


object NeighborGetUtils {

  def partitionGet(psContext: PSContext, partParam: PartitionGetParam,
                   useTrunc: Boolean, truncLen: Int):
  PartitionGetResult = {
    val param = partParam.asInstanceOf[GeneralPartGetParam]
    val keyPart = param.getIndicesPart
    // Long type node id
    val nodeIds = keyPart.asInstanceOf[ILongKeyPartOp].getKeys
    val row = GraphMatrixUtils.getPSLongKeyRow(psContext, param)
    // Get data
    val data = new Array[IElement](nodeIds.length)
    for (i <- 0 until nodeIds.length) {
      val element = row.get(nodeIds(i))
      if (useTrunc) {
        val neighbors = element.asInstanceOf[LongArrayElement].getData
        val neighborsShuffle = scala.util.Random.shuffle(neighbors.toSeq).toArray
        element.asInstanceOf[LongArrayElement].setData(neighborsShuffle.slice(0, truncLen))
      }
      data(i) = element
    }
    val meta = psContext.getMatrixMetaManager.getMatrixMeta(param.getMatrixId)
    try new PartGeneralGetResult(meta.getValueClass, nodeIds, data)
    catch {
      case e: ClassNotFoundException =>
        throw new AngelException("Can not get value class ")
    }
  }
}