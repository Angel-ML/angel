package com.tencent.angel.graph.embedding.node2vec

import java.util

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult, PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import scala.collection.JavaConversions._


/**
 * Sample the neighbor with truncation if needed
 */

class GetNeighborWithTrunc(param: LongKeysGetParam, useTrunc: Boolean, truncLen: Int, dynamicInitNeighbor: Boolean=false)
  extends GetFunc(param) {

  def this() = this(null, false, -1)

  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult =
    NeighborGetUtils.partitionGet(psContext, partParam, useTrunc, truncLen, dynamicInitNeighbor)

  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val emp = new Array[Long](0)
    var resultSize = 0
    for (result <- partResults) {
      resultSize += result.asInstanceOf[PartGeneralGetResult].getNodeIds.length
    }
    val nodeIdToNeighbors = new Long2ObjectOpenHashMap[Array[Long]](resultSize)
    for (result <- partResults) {
      val partResult = result.asInstanceOf[PartGeneralGetResult]
      val nodeIds = partResult.getNodeIds
      val data = partResult.getData
      for (i <- 0 until nodeIds.length) {
        if (data(i) != null) {
          val neighbors = data(i).asInstanceOf[LongArrayElement].getData
          if (neighbors.length > 0) {
            nodeIdToNeighbors.put(nodeIds(i), neighbors)
          }
          else nodeIdToNeighbors.put(nodeIds(i), emp)
        }
        else nodeIdToNeighbors.put(nodeIds(i), emp)
      }
    }
    new GetLongsResult(nodeIdToNeighbors)
  }
}
