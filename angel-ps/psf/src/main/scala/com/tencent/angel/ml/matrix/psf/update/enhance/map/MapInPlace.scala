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


package com.tencent.angel.ml.matrix.psf.update.enhance.map

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor
import com.tencent.angel.ml.math2.ufuncs.expression.Unary
import com.tencent.angel.ml.matrix.psf.update.enhance.map.func.MapFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.{MFUpdateFunc, MFUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerRow, ServerRowUtils}

/**
  * It is a Map function which applies `MapFunc` to `fromId` row and saves the result to `toId` row
  */
class MapInPlace(param: MFUpdateParam) extends MFUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, func: MapFunc) = this(new MFUpdateParam(matrixId, Array(rowId), func))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], func: Serialize): Unit = {
    val op = func.asInstanceOf[Unary]
    assert(op.isInplace, "not inplace op")
    rows(0).startWrite()
    try {
      UnaryExecutor.apply(ServerRowUtils.getVector(rows(0)), op)
    } finally {
      rows(0).endWrite()
    }
  }
}
