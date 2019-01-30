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


package com.tencent.angel.ml.core.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.psf.optimizer.PGDUpdateFunc
import com.tencent.angel.psagent.PSAgentContext

class SGD(stepSize: Double) extends Optimizer(stepSize) {
  override protected var numSlot: Int = 1

  override def update(matrixId: Int, numFactors: Int, epoch: Int): Future[VoidResult] = {
    update(matrixId, numFactors, epoch, 1)
  }

  override def update(matrixId: Int, numFactors: Int, epoch: Int, batchSize: Int): Future[VoidResult] = {
    val func = new PGDUpdateFunc(matrixId, numFactors, lr, regL1Param, regL2Param, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def toString: String = {
    s"SGD lr=$lr regL2=$regL2Param regL1=$regL1Param"
  }
}
