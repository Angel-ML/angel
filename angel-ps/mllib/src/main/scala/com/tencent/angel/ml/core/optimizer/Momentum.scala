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

import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.mlcore.utils.OptimizerKeys
import com.tencent.angel.mlcore.variable.Variable
import com.tencent.angel.ml.core.variable.PSVariable
import com.tencent.angel.ml.psf.optimizer.MomentumUpdateFunc
import com.tencent.angel.mlcore.optimizer.{Optimizer, OptimizerProvider}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST._
import org.json4s.JsonDSL._


class Momentum(override var lr: Double, val momentum: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[Momentum])

  override val numSlot: Int = 2

  override def update[T](variable: Variable, epoch: Int, batchSize: Int = 1): Future[T] = {
    val matrixId = variable.asInstanceOf[PSVariable].getMatrixId
    val func = new MomentumUpdateFunc(matrixId, variable.asInstanceOf[PSVariable].numFactors,
      momentum, lr, regL2Param, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"Momentum momentum=$momentum lr=$lr regL2=$regL2Param"
  }

  override def toJson: JObject = {
    (OptimizerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (OptimizerKeys.momentumKey -> momentum)
  }
}

object Momentum {
  def fromJson(jast: JObject, provider: OptimizerProvider)(implicit conf: SharedConf): Momentum = {
    val psProvider = provider.asInstanceOf[PSOptimizerProvider]
    assert(psProvider.fieldEqualClassName[Momentum](jast, OptimizerKeys.typeKey))
    val lr = conf.getDouble(MLCoreConf.ML_LEARN_RATE, MLCoreConf.DEFAULT_ML_LEARN_RATE)
    val moment = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM, MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)

    val regL1Param: Double = conf.getDouble(MLCoreConf.ML_REG_L1, MLCoreConf.DEFAULT_ML_REG_L1)
    val regL2Param: Double = conf.getDouble(MLCoreConf.ML_REG_L2, MLCoreConf.DEFAULT_ML_REG_L2)
    val opt = new Momentum(lr, psProvider.extract[Double](jast, OptimizerKeys.momentumKey, Some(moment)).get)
    opt.setRegL1Param(regL1Param).setRegL2Param(regL2Param)
  }
}

