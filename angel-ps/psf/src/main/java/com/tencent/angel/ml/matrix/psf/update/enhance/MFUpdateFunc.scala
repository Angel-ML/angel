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

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.matrix.psf.Utils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc}
import com.tencent.angel.ps.storage.vector.ServerRow


/**
  * `MFUpdateFunc` is a PSF Update for multi rows in matrix with a user-defined function.
  * Constructor's Parameters include int[] `rowIds` and Serialize `func`, which correspond to
  * ServerDenseDoubleRow[] `rows` and Serialize `func` in `doUpdate` interface respectively.
  *
  * That is the length of `rowIds` and `rows` is exactly the same, rows[i] is the content of
  * rowIds[i] row in matrix.
  */
abstract class MFUpdateFunc(param: MFUpdateParam) extends UpdateFunc(param) {
  def this(matrixId: Int, rowIds: Array[Int], func: Serialize) =
    this(new MFUpdateParam(matrixId, rowIds, func))

  def this() = this(null)


  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      val mf = partParam.asInstanceOf[MFUpdateParam.MFPartitionUpdateParam]
      val rowIds = mf.getRowIds
      if (Utils.withinPart(partParam.getPartKey, rowIds)) {
        update(rowIds.map(part.getRow), mf.getFunc)
      }
    }
  }

  protected def update(rows: Array[ServerRow], func: Serialize): Unit
}
