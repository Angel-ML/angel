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

import com.tencent.angel.ml.core.utils.paramsutils.ParamKeys
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.psf.optimizer.MomentumUpdateFunc
import com.tencent.angel.psagent.PSAgentContext
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

class Momentum(stepSize: Double, val momentum: Double) extends Optimizer(stepSize) {
  override protected var numSlot: Int = 2

  override def update(matrixId: Int, numFactors: Int, epoch: Int = 0): Future[VoidResult] = {
    update(matrixId, numFactors, epoch, 1)
  }

  override def update(matrixId: Int, numFactors: Int, epoch: Int, batchSize: Int): Future[VoidResult] = {
    val func = new MomentumUpdateFunc(matrixId, numFactors, momentum, lr, regL2Param, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def toString: String = {
    s"Momentum momentum=$momentum lr=$lr regL2=$regL2Param"
  }

  override def toJson: JObject = {
    (ParamKeys.typeName -> s"${this.getClass.getSimpleName}") ~
      (ParamKeys.momentum -> momentum)
  }
}
