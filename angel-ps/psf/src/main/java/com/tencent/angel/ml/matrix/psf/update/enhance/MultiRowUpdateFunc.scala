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

import com.tencent.angel.ml.matrix.psf.Utils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc}
import com.tencent.angel.ps.storage.vector._


/**
 * `FullUpdateFunc` is a PSF Update for the whole matrix in matrix with a user-defined function.
 */

abstract class MultiRowUpdateFunc(param: MultiRowUpdateParam) extends UpdateFunc(param) {
  def this(matrixId: Int, rowIds: Array[Int], values: Array[Array[Double]]) = {
    this(new MultiRowUpdateParam(matrixId, rowIds, values))
  }
  def this() = this(null)


  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      val mf = partParam.asInstanceOf[MultiRowUpdateParam.MultiRowPartitionUpdateParam]
      val rowIds = mf.getRowIds
      val values = mf.getValues
      for (i <- rowIds.indices){
        if (Utils.withinPart(partParam.getPartKey, Array(rowIds(i))))
          update(part.getRow(i), values(i))
      }
    }
  }

  def update(row: ServerRow, values: Array[Double])
}
