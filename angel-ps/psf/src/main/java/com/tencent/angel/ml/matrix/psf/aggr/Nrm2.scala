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

import com.tencent.angel.ml.math2.vector.{DoubleVector, FloatVector, IntVector, LongVector}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc
import com.tencent.angel.ps.storage.vector.ServerRow;

/**
  * `Nrm2` will return 2-Norm of the `rowId` row in `matrixId` matrix.
  * Row is a Array of double, and `Nrm2` is \sqrt (\sum { row(i) * row(i) })
  */
class Nrm2(matrixId: Int, rowId: Int) extends UnaryAggrFunc(matrixId, rowId) {
  def this() = this(-1, -1)

  override protected def processRow(row: ServerRow): Double = {
    row.getSplit match {
      case s: DoubleVector => s.norm()
      case s: FloatVector => s.norm()
      case s: LongVector => s.norm()
      case s: IntVector => s.norm()
    }
  }

  override protected def mergeInit: Double = Double.MinValue

  override protected def mergeOp(a: Double, b: Double): Double = math.max(a, b)
}
