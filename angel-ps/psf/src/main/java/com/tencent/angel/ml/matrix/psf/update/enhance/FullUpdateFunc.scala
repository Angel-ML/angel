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


package com.tencent.angel.ml.matrix.psf.update.enhance

import com.tencent.angel.PartitionKey
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc}
import com.tencent.angel.ps.storage.matrix.ServerPartition
import com.tencent.angel.ps.storage.vector._


/**
  * `FullUpdateFunc` is a PSF Update for the whole matrix in matrix with a user-defined function.
  */

abstract class FullUpdateFunc(param: FullUpdateParam) extends UpdateFunc(param) {
  def this(matrixId: Int, values: Array[Double]) = this(new FullUpdateParam(matrixId, values))

  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam) {
    val part = psContext.getMatrixStorageManager
      .getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      val ff = partParam.asInstanceOf[FullUpdateParam.FullPartitionUpdateParam]
      update(part, partParam.getPartKey, ff.getValues);
    }
  }

  private def update(part: ServerPartition, key: PartitionKey, values: Array[Double]) {
    val startRow = key.getStartRow
    val endRow = key.getEndRow
    part.getRowType match {
      case RowType.T_DOUBLE_DENSE =>
        doUpdate(Array.range(startRow, endRow).map(part.getRow(_).asInstanceOf[ServerIntDoubleRow]), values)
      case RowType.T_FLOAT_DENSE =>
        doUpdate(Array.range(startRow, endRow).map(part.getRow(_).asInstanceOf[ServerIntFloatRow]), values.map(_.toFloat))
      case RowType.T_LONG_DENSE =>
        doUpdate(Array.range(startRow, endRow).map(part.getRow(_).asInstanceOf[ServerIntLongRow]), values.map(_.toLong))
      case RowType.T_INT_DENSE =>
        doUpdate(Array.range(startRow, endRow).map(part.getRow(_).asInstanceOf[ServerIntIntRow]), values.map(_.toInt))
      case other => throw new AngelException(s"not supported operation for rowType = $other")
    }
  }

  def doUpdate(rows: Array[ServerIntDoubleRow], values: Array[Double])

  def doUpdate(rows: Array[ServerIntFloatRow], values: Array[Float])

  def doUpdate(rows: Array[ServerIntLongRow], values: Array[Long])

  def doUpdate(rows: Array[ServerIntIntRow], values: Array[Int])
}
