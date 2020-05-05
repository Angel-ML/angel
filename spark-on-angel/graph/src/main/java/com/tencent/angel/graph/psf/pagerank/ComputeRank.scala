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
package com.tencent.angel.graph.psf.pagerank

import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.FloatVector
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

class ComputeRank(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowIds: Array[Int], initRank: Float, resetProb: Float) =
    this(new MMUpdateParam(matrixId, rowIds, Array[Double](initRank, resetProb)))

  def this() = this(null)

  override protected
  def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val inMsgs = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val outMsgs = ServerRowUtils.getVector(rows(1).asInstanceOf[ServerLongFloatRow])
    val ranks = ServerRowUtils.getVector(rows(2).asInstanceOf[ServerLongFloatRow])
    Ufuncs.ipagerank(ranks, outMsgs, scalars(0).toFloat, scalars(1).toFloat)
    switchWriteAndRead(inMsgs, outMsgs)
  }

  def switchWriteAndRead(inMsgs: FloatVector, outMsgs: FloatVector): Unit = {
    val tmp = inMsgs.getStorage
    inMsgs.setStorage(outMsgs.getStorage)
    outMsgs.setStorage(tmp)
    tmp.clear()
  }

}
