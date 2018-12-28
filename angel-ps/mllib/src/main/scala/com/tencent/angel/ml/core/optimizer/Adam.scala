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
import com.tencent.angel.ml.psf.optimizer.AdamUpdateFunc
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory

import scala.collection.mutable

class Adam(override val stepSize: Double,
           var gamma: Double = 0.99,
           var beta: Double = 0.9,
           var epsilon: Double = 1e-7) extends GradientDescent(stepSize) {

  val LOG = LogFactory.getLog(classOf[Adam])

  override def resetParam(paramMap: mutable.Map[String, Double]): Unit = {
    super.resetParam(paramMap)
    gamma = paramMap.getOrElse("gamma", gamma)
    beta = paramMap.getOrElse("beta", beta)
    epsilon = paramMap.getOrElse("epsilon", epsilon)
  }

  override def update(matrixId: Int, numFactors: Int, epoch: Int): Future[VoidResult] = {

    val func = new AdamUpdateFunc(matrixId, numFactors, gamma, epsilon, beta, lr, regL2Param, epoch)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def update(matrixId: Int, numFactors: Int, epoch: Int, sampleNum: Int): Future[VoidResult] = {
    val func = new AdamUpdateFunc(matrixId, numFactors, gamma, epsilon, beta, lr, regL2Param, epoch, sampleNum)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def toString: String = {
    s"Adam gamma=$gamma beta=$beta epsilon=$epsilon"
  }
}
