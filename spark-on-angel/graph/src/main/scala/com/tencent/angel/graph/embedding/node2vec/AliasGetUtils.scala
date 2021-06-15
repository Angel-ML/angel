package com.tencent.angel.graph.embedding.node2vec

import com.tencent.angel.exception.AngelException
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base.{GeneralPartGetParam, PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp


object AliasGetUtils {

  def partitionGet(psContext: PSContext, partParam: PartitionGetParam,
                   useTrunc: Boolean, truncLen: Int): PartitionGetResult = {
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
        val neighbors = element.asInstanceOf[AliasElement].getNeighborIds
        val accepts = element.asInstanceOf[AliasElement].getAccept
        val alias = element.asInstanceOf[AliasElement].getAlias
        val entries = neighbors.zip(accepts).zip(alias).map(e => (e._1._1, e._1._2, e._2))
        val truncEntries = if (truncLen < neighbors.length) {
          val entriesShuffle = scala.util.Random.shuffle(entries.toSeq).toArray
          entriesShuffle.slice(0, truncLen)
        } else {
          entries
        }
        val (neighborsTrunc, acceptTrunc, aliasTrunc) = truncEntries.unzip3
        element.asInstanceOf[AliasElement].setNeighborIds(neighborsTrunc)
        element.asInstanceOf[AliasElement].setAccept(acceptTrunc)
        element.asInstanceOf[AliasElement].setAlias(aliasTrunc)
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