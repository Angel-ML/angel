package com.tencent.angel.ml.matrix.psf.get.single

import java.util.List
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.impl.PSContext
import com.tencent.angel.psagent.matrix.ResponseType
import com.tencent.angel.psagent.matrix.transport.adapter.RowSplitCombineUtils
import scala.collection.JavaConversions._

/**
  * Get a matrix row function implements by the get udf.
  */
class GetRowFunc(param: GetParam) extends GetFunc(param) {
  def this() = this(null)

  def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val param = partParam.asInstanceOf[PartitionGetRowParam]
    val row = PSContext.get.getMatrixPartitionManager.getRow(param.getMatrixId, param.getRowIndex,
                                                              param.getPartKey.getPartitionId)
    new PartitionGetRowResult(row)
  }

  def merge(partResults: List[PartitionGetResult]): GetResult = {
    val rowSplits = partResults.map(_.asInstanceOf[PartitionGetRowResult].getRowSplit)

    val getRowParam: GetRowParam = getParam.asInstanceOf[GetRowParam]
    return new GetRowResult(ResponseType.SUCCESS,
                            RowSplitCombineUtils.combineServerRowSplits(rowSplits,
                                                                        getRowParam.getMatrixId,
                                                                        getRowParam.getRowIndex))
  }
}
