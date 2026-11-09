package com.tencent.angel.spark.ml.psf.node2vec

import com.tencent.angel.exception.AngelException
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement
import com.tencent.angel.graph.model.neighbor.samplewithtrunc.PartSampleTruncParam
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp

import scala.util.Random

object NeighborGetUtils {

  def partitionGet(psContext: PSContext, partParam: PartitionGetParam):
  PartitionGetResult = {
    val param = partParam.asInstanceOf[PartSampleTruncParam]
    val keyPart = param.getIndicesPart
    // Long type node id
    val nodeIds = keyPart.asInstanceOf[ILongKeyPartOp].getKeys
    val isTrunc = param.getIsTrunc
    val truncLen = param.getTruncLen
    val dynamicInitNeighbor = param.getDynamicInitNeighbor
    val row = GraphMatrixUtils.getPSLongKeyRow(psContext, param)
    // Get data
    val r = new Random()
    val data = new Array[IElement](nodeIds.length)
    for (i <- 0 until nodeIds.length) {
      val element = row.get(nodeIds(i))
      if (isTrunc) {
        val neighbors = if (dynamicInitNeighbor) element.asInstanceOf[DynamicSimpleNeighborElement].getData
        else element.asInstanceOf[LongArrayElement].getData
        if (truncLen >= neighbors.length) {
          data(i) = if (dynamicInitNeighbor) new LongArrayElement(neighbors) else element
        } else {
          val startIndex = r.nextInt(neighbors.length)
          if (startIndex <= neighbors.length - truncLen) {
            data(i) = new LongArrayElement(neighbors.slice(startIndex, startIndex + truncLen))
          } else {
            val temp = new Array[Long](truncLen)
            System.arraycopy(neighbors, startIndex, temp, 0, neighbors.length - startIndex)
            System.arraycopy(neighbors, 0, temp, neighbors.length - startIndex, truncLen - (neighbors.length - startIndex))
            data(i) = new LongArrayElement(temp)
          }
        }
      } else {
        data(i) = if (dynamicInitNeighbor) new LongArrayElement(element.asInstanceOf[DynamicSimpleNeighborElement].getData) else element
      }
    }
    // val meta = psContext.getMatrixMetaManager.getMatrixMeta(param.getMatrixId)
    try new PartGeneralGetResult(classOf[LongArrayElement], nodeIds, data)
    catch {
      case e: ClassNotFoundException =>
        throw new AngelException("Can not get value class ")
    }
  }
}