package com.tencent.angel.spark.ml.psf.node2vec

import com.tencent.angel.exception.AngelException
import com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor.AliasElement
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult
import com.tencent.angel.graph.model.neighbor.samplewithtrunc.PartSampleTruncParam
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp


object AliasGetUtils {

  def partitionGet(psContext: PSContext, partParam: PartitionGetParam): PartitionGetResult = {
    val param = partParam.asInstanceOf[PartSampleTruncParam]
    val keyPart = param.getIndicesPart
    // Long type node id
    val nodeIds = keyPart.asInstanceOf[ILongKeyPartOp].getKeys
    val row = GraphMatrixUtils.getPSLongKeyRow(psContext, param)
    val isTrunc = param.getIsTrunc
    val truncLen = param.getTruncLen
    // Get data
    val data = new Array[IElement](nodeIds.length)
    for (i <- 0 until nodeIds.length) {
      val element = row.get(nodeIds(i))
      if (isTrunc) {
        val neighborsLen = element.asInstanceOf[AliasElement].getNodesNum
        assert(neighborsLen <= truncLen, s" node=${nodeIds(i)}, for weighted node2vec walking, expected neighborsLen<=$truncLen, got $neighborsLen.")
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