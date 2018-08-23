/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.matrix.psf.aggr.enhance

import com.tencent.angel.ml.matrix.psf.Utils
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult, PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.storage.vector.ServerRow


/**
  * This is abstract class of Unary Aggregate Function of POF (PS Oriented Function),
  * other aggregate function will extend `UnaryAggrFunc` and implement `doProcessRow`.
  */
abstract class UnaryAggrFunc(val matrixId: Int, val rowId: Int)
  extends GetFunc(new UnaryAggrParam(matrixId, rowId)) {
  def this() = this(-1, -1)

  override def partitionGet(partKey: PartitionGetParam): PartitionGetResult = {
    val part = psContext.getMatrixStorageManager.getPart(partKey.getMatrixId, partKey.getPartKey.getPartitionId)
    if (part != null) {
      val rowId = partKey.asInstanceOf[UnaryAggrParam.UnaryPartitionAggrParam].getRowId
      if (Utils.withinPart(part.getPartitionKey, Array[Int](rowId))) {
        val row = part.getRow(rowId)
        val result = processRow(row)
        return new ScalarPartitionAggrResult(result)
      }
    }
    null
  }

  override def merge(partResults: java.util.List[PartitionGetResult]): GetResult = {
    var aggRet = mergeInit
    val retIter = partResults.iterator()
    while (retIter.hasNext) {
      val partRet = retIter.next()
      if (null != partRet)
        aggRet = mergeOp(aggRet, partRet.asInstanceOf[ScalarPartitionAggrResult].result)
    }
    new ScalarAggrResult(aggRet)
  }

  protected def mergeInit: Double

  protected def mergeOp(a: Double, b: Double): Double

  protected def processRow(row: ServerRow): Double
}
