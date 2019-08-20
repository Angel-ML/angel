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
package com.tencent.angel.ml.core.variable


import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.mlcore.variable._
import com.tencent.angel.psagent.PSAgentContext


class PSVecVariable(name: String,
                    length: Long,
                    val validIndexNum: Long,
                    updater: Updater,
                    rowType: RowType,
                    formatClassName: String,
                    allowPullWithIndex: Boolean)
                   (implicit  conf: SharedConf, variableManager: VariableManager, cilsImpl: CILSImpl)
  extends PSVariable(name, rowType, updater, formatClassName, allowPullWithIndex) with VecVariable {
  override val numFactors: Int = 1
  override protected var vector: Vector = _

  def getMatrixCtx: MatrixContext = {
    if (ctx == null) {
      PSMatrixUtils.createPSMatrixCtx(name, numSlot + 1, length, rowType,
        formatClassName, validIndexNum)
    } else {
      ctx
    }
  }

  protected override def rowsSaved(withSlot: Boolean): Array[Int] = {
    if (withSlot && numSlot > 0) {
      (0 until numSlot).toArray
    } else {
      Array(0)
    }
  }

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
      val randFunc = new RandomNormal(matrixId, 0, 1, mean, stddev)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
    }
  }

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
    if (indices != null) {
      vector = PSMatrixUtils.getRowWithIndex(epoch, matrixId, 0, indices)(mean, stddev)
    } else {
      vector = PSMatrixUtils.getRow(epoch, matrixId, 0)
    }
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    if (numSlot <= 0) {
      val vector = grad.getRow(0).imul(-alpha)
      vector.setMatrixId(matrixId)
      vector.setRowId(0)
      PSMatrixUtils.incrementRow(matrixId, 0, vector)
    } else {
      val vector = grad.getRow(0)
      vector.setMatrixId(matrixId)
      vector.setRowId(numSlot)
      PSMatrixUtils.incrementRow(matrixId, numSlot, vector)
    }
  }


}
