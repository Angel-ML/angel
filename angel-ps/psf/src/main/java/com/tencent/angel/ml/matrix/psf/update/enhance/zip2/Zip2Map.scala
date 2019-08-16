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


package com.tencent.angel.ml.matrix.psf.update.enhance.zip2

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.func.Zip2MapFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.{MFUpdateFunc, MFUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerRow, ServerRowUtils}


/**
  * It is a Zip2Map function which applies `Zip2MapFunc` to `fromId1` and `fromId2` row and saves
  * the result to `toId` row.
  */
class Zip2Map(param: MFUpdateParam) extends MFUpdateFunc(param) {
  def this(matrixId: Int, fromId1: Int, fromId2: Int, toId: Int, func: Zip2MapFunc) {
    this(new MFUpdateParam(matrixId, Array(fromId1, fromId2, toId), func))
  }

  def this(matrixId: Int, yId: Int, xId: Int, func: Zip2MapFunc) {
    this(matrixId, yId, xId, yId, func)
  }

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], func: Serialize): Unit = {
    val op = func.asInstanceOf[Zip2MapFunc]
    if (op.isInplace) {
      assert(rows(0) == rows(2), "Zip2Map with inplace op, but toId != fromId1")
      rows(0).startWrite()
      try {
        BinaryExecutor.apply(ServerRowUtils.getVector(rows(0)), ServerRowUtils.getVector(rows(1)), op)
      } finally {
        rows(0).endWrite()
      }
    } else {
      val from1 = ServerRowUtils.getVector(rows(0))
      val from2 = ServerRowUtils.getVector(rows(1))
      rows(2).startWrite()
      try {
        ServerRowUtils.setVector(rows(2), BinaryExecutor.apply(from1, from2, op))
      } finally {
        rows(2).endWrite()
      }
    }

  }
}
