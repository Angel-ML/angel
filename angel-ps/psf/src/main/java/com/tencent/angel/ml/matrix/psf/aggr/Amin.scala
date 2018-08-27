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

import com.tencent.angel.ml.math2.storage.{DoubleVectorStorage, FloatVectorStorage, IntVectorStorage, LongVectorStorage}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc
import com.tencent.angel.ps.storage.vector.ServerRow

/**
  * `Amin` will aggregate the minimum absolute value of the `rowId` row in `matrixId` matrix.
  * For example, if the content of `rowId` row in `matrixId` matrix is [0.3, -1.1, 2.0, 10.1],
  * the aggregate result of `Amin` is 0.3.
  */
class Amin(matrixId: Int, rowId: Int) extends UnaryAggrFunc(matrixId, rowId) {
  def this() = this(-1, -1)

  override protected def processRow(row: ServerRow): Double = {
    val v = row.getSplit
    if (v.dim() != v.getSize) 0.0 else {
      v.getStorage match {
        case s: DoubleVectorStorage => s.getValues.map(math.abs).min
        case s: FloatVectorStorage => s.getValues.map(math.abs).min
        case s: LongVectorStorage => s.getValues.map(math.abs).min
        case s: IntVectorStorage => s.getValues.map(math.abs).min
      }
    }
  }

  override protected def mergeInit: Double = Double.MaxValue

  override protected def mergeOp(a: Double, b: Double): Double = math.min(a, b)
}
