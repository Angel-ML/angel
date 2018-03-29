/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.optimizer2.sgd

import java.util

import com.tencent.angel.ml.math.TUpdate
import com.tencent.angel.ml.optimizer2.utils.oputils.{ScalarExpr, TOperation}
import com.tencent.angel.ml.optimizer2.utils.{ExecuteUtils, OptUtils}
import com.tencent.angel.ml.optimizer2.{OptModel, Optimizer}

import scala.collection.JavaConversions._

class Momentum(batchSize: Int, numUpdatePerEpoch: Int, lr: Double,
               var rho: Double = 0.8, val l2Reg: Map[String, Double] = null)
  extends Optimizer(batchSize, numUpdatePerEpoch, lr) {
  private var moment: util.HashMap[String, TUpdate] = _

  override def updateLocal(model: OptModel, numSample: Int, iterCount: Int): Unit = {
    val gradWithReg = if (l2Reg == null) {
      ExecuteUtils.executeScalar(grad, new ScalarExpr(alpha = 1.0f / numSample, top = TOperation.Mul, isInplace = true))
    } else {
      val oneMap = localParams.map { case (name: String, _) => name -> 1.0 / numSample }.toMap
      OptUtils.linear(localParams, l2Reg, grad, oneMap)
    }

    val rhoMap = localParams.map { case (name: String, _) =>
      name -> (if (model.getIndexFlag.get(name)) rho else 0.0)
    }.toMap
    val lrMap = localParams.map { case (name: String, _) => name -> 1.0 }.toMap
    OptUtils.ilinear(moment, rhoMap, gradWithReg, lrMap)

    OptUtils.iaxpy(localParams, moment, -lr)
  }

  override def initialLocal(model: OptModel, local: util.HashMap[String, TUpdate], global: util.HashMap[String, TUpdate]): Unit = {
    if (moment == null && l2Reg != null) {
      moment = OptUtils.emptyLike(global)
    } else if (moment == null && l2Reg == null) {
      moment = model.getZeroParams
    } else {
      moment = OptUtils.axpy(global, local, -1.0)
    }
  }
}
