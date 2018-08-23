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
import com.tencent.angel.ml.matrix.psf.update.enhance.map.func.MapFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.{MFUpdateFunc, MFUpdateParam}
import com.tencent.angel.ps.storage.vector.ServerRow

/**
  * It is a MapWithIndex function which applies `MapWithIndexFunc` to `fromId` row and saves the result to `toId` row
  */
class MapWithIndex(param: MFUpdateParam) extends MFUpdateFunc(param) {
  def this(matrixId: Int, fromId: Int, toId: Int, func: MapFunc) =
    this(new MFUpdateParam(matrixId, Array(fromId, toId), func))

  def this() = this(null)

  def update(rows: Array[ServerRow], func: Serialize) {
    val op = func.asInstanceOf[MapFunc]
    assert(op.isOrigin, "not origin op")
    val mapValue = UnaryExecutor.apply(rows(0).getSplit, op)
    rows(1).startWrite()
    try {
      rows(1).setSplit(mapValue)
    } finally {
      rows(1).endWrite()
    }
  }
}
