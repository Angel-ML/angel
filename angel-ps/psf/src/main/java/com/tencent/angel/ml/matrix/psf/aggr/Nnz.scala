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


package com.tencent.angel.ml.matrix.psf.aggr

;

import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc
import com.tencent.angel.ps.storage.vector.ServerRow;

/**
  * `Nnz` will return the number of non-zero of the `rowId` row in `matrixId` matrix.
  * For example, if the content of `rowId` row in `matrixId` matrix is [0.0, -11.0, 0.0, 10.1],
  * the aggregate result of `Nnz` is 2 .
  */
class Nnz(matrixId: Int, rowId: Int) extends UnaryAggrFunc(matrixId, rowId) {
  def this() = this(-1, -1)

  override protected def processRow(row: ServerRow): Double = {
    row.getSplit match {
      case s: IntKeyVector => s.getDim - s.numZeros()
      case s: LongKeyVector => s.getDim - s.numZeros()
    }
  }

  override protected def mergeInit: Double = 0.0

  override protected def mergeOp(a: Double, b: Double): Double = a + b


}
