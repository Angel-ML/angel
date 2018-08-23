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


import com.tencent.angel.ml.math2.vector.{DoubleVector, FloatVector, IntVector, LongVector}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc
import com.tencent.angel.ps.storage.vector.ServerRow

/**
  * `Max` will aggregate the maximum value of the `rowId` row in `matrixId` matrix.
  * For example, if the content of `rowId` row in `matrixId` matrix is [0.3, -11.0, 2.0, 10.1],
  * the aggregate result of `Max` is 10.1 .
  */
class Max(matrixId: Int, rowId: Int) extends UnaryAggrFunc(matrixId, rowId) {
  def this() = this(-1, -1)

  override protected def processRow(row: ServerRow): Double = {
    row.getSplit match {
      case s: DoubleVector => s.max()
      case s: FloatVector => s.max()
      case s: LongVector => s.max()
      case s: IntVector => s.max()
    }
  }

  override protected def mergeInit: Double = Double.MinValue

  override protected def mergeOp(a: Double, b: Double): Double = math.max(a, b)

}
