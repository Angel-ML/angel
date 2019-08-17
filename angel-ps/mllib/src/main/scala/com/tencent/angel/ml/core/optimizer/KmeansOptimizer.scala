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
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.utils.OptimizerKeys
import com.tencent.angel.mlcore.variable.Variable
import com.tencent.angel.ml.core.variable.PSVariable
import com.tencent.angel.ml.psf.optimizer.KmeansUpdateFunc
import com.tencent.angel.mlcore.optimizer.{Optimizer, OptimizerProvider}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.{JField, JObject, JString}

import scala.reflect.ClassTag

class KmeansOptimizer() extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[KmeansOptimizer])
  override var lr: Double = _
  override val numSlot: Int = 1

  override def update[T](variable: Variable, epoch: Int, batchSize: Int): Future[T] = {
    val matrixId = variable.asInstanceOf[PSVariable].getMatrixId
    val func = new KmeansUpdateFunc(matrixId, variable.asInstanceOf[PSVariable].numFactors, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"KmeansOptimizer"
  }

  override def toJson: JObject = {
    JObject(JField(OptimizerKeys.typeKey, JString(s"${this.getClass.getSimpleName}")))
  }
}


object KmeansOptimizer {
  def fromJson(jast: JObject, provider: OptimizerProvider)(implicit conf: SharedConf): KmeansOptimizer = {
    val psProvider = provider.asInstanceOf[PSOptimizerProvider]
    assert(psProvider.fieldEqualClassName[KmeansOptimizer](jast, OptimizerKeys.typeKey))
    new KmeansOptimizer()
  }
}
