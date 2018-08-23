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

import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc}
import com.tencent.angel.ps.storage.vector._

/**
  * `MMUpdateFunc` is a POF updater for a row in matrix with multi double parameter.
  *
  * Constructor's Parameters include int[] `rowIds` and double[] `scalars`, which correspond to
  * ServerDenseDoubleRow[] `rows` and double[] `scalars` in `doUpdate` interface respectively.
  *
  * That is the length of `rowIds` and `rows` is exactly the same, rows[i] is the content of
  * rowIds[i] row in matrix.
  */
abstract class MMUpdateFunc(param: MMUpdateParam) extends UpdateFunc(param) {
  def this() = this(null)

  def this(matrixId: Int, rowIds: Array[Int], scalars: Array[Double]) =
    this(new MMUpdateParam(matrixId, rowIds, scalars))

  def this(matrixId: Int, startId: Int, length: Int, scalars: Array[Double]) =
    this(new MMUpdateParam(matrixId, startId, length, scalars))


  override def partitionUpdate(partParam: PartitionUpdateParam) {
    val part = psContext.getMatrixStorageManager.getPart(
      partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      val vs2 = partParam.asInstanceOf[MMUpdateParam.MMPartitionUpdateParam]
      val rowIds = vs2.getRowIds
      val rows = new Array[ServerRow](rowIds.length)
      for (i <- 0 until rowIds.length) {
        rows(i) = part.getRow(rowIds(i))
      }
      update(rows, vs2.getScalars)
    }
  }

  protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit
}
