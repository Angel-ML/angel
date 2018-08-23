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
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ps.storage.vector.ServerRow


/**
  * This is abstract class of Binary Aggregate Function of POF (PS Oriented Function),
  * other aggregate function will extend `BinaryAggrFunc` and implement `doProcessRow`.
  * This function will process two rows in the same matrix.
  */
abstract class BinaryAggrFunc(matrixId: Int, rowId1: Int, rowId2: Int)
  extends GetFunc(new BinaryAggrParam(matrixId, rowId1, rowId2)) {
  def this() = this(-1, -1, -1)

  override def partitionGet(partKey: PartitionGetParam): PartitionGetResult = {
    val part = psContext.getMatrixStorageManager.getPart(partKey.getMatrixId, partKey.getPartKey.getPartitionId)
    val rowId1 = partKey.asInstanceOf[BinaryAggrParam.BinaryPartitionAggrParam].getRowId1
    val rowId2 = partKey.asInstanceOf[BinaryAggrParam.BinaryPartitionAggrParam].getRowId2
    if (Utils.withinPart(partKey.getPartKey, Array[Int](rowId1, rowId2))) if (part != null) {
      val row1 = part.getRow(rowId1)
      val row2 = part.getRow(rowId2)
      if (row1 != null && row2 != null) {
        val result = processRows(row1, row2)
        return new ScalarPartitionAggrResult(result)
      }
    }
    null
  }

  protected def processRows(row1: ServerRow, row2: ServerRow): Double
}
