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


package com.tencent.angel.ml.matrix.psf.update

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.update.enhance.{FullUpdateFunc, MultiRowUpdateFunc, MultiRowUpdateParam}
import com.tencent.angel.ps.storage.vector._
import com.tencent.angel.ps.storage.vector.func.{DoubleElemUpdateFunc, FloatElemUpdateFunc, IntElemUpdateFunc, LongElemUpdateFunc}


/**
 * `FullFill` a matrix with `value`
 */
class Fill(params: MultiRowUpdateParam)
  extends MultiRowUpdateFunc(params) {

  def this(matrixId: Int, rowIds: Array[Int], values: Array[Double]) = {
    this(new MultiRowUpdateParam(matrixId, rowIds, values.map(Array(_))))
  }

  def this() = this(null)

  override def update(row: ServerRow, values: Array[Double]): Unit = {
    row.startWrite()
    try {
      row match {
        case r: ServerIntDoubleRow =>
          r.elemUpdate(new DoubleElemUpdateFunc {
            override def update(): Double = {
              values(0)
            }
          })
        case r: ServerIntFloatRow =>
          r.elemUpdate(new FloatElemUpdateFunc {
            override def update(): Float = {
              values(0).toFloat
            }
          })
        case r: ServerIntLongRow =>
          r.elemUpdate(new LongElemUpdateFunc {
            override def update(): Long = {
              values(0).toLong
            }
          })
        case r: ServerIntIntRow =>
          r.elemUpdate(new IntElemUpdateFunc {
            override def update(): Int = {
              values(0).toInt
            }
          })
        case r => throw new AngelException(s"not supported for row type: ${r.getRowType}")
      }
    } finally {
      row.endWrite()
    }
  }
}
