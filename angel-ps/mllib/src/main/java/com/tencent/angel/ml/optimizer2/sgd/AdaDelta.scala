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
import com.tencent.angel.ml.optimizer2.utils.oputils._
import com.tencent.angel.ml.optimizer2.utils.{ExecuteUtils, OptUtils}
import com.tencent.angel.ml.optimizer2.{OptModel, Optimizer}

import scala.collection.JavaConversions._

// http://www.cnblogs.com/neopenx/p/4768388.html
// https://zh.gluon.ai/chapter_optimization/adadelta-scratch.html
// https://arxiv.org/pdf/1212.5701.pdf

class AdaDelta(batchSize: Int, numUpdatePerEpoch: Int, lr: Double, val rho: Double,
               val l1Reg: Map[String, Double] = null, val l2Reg: Map[String, Double] = null)
  extends Optimizer(batchSize, numUpdatePerEpoch, lr) {
  private var sumDelta2: util.HashMap[String, TUpdate] = _
  private var sumGrad2: util.HashMap[String, TUpdate] = _

  override def updateLocal(model: OptModel, numSample: Int, iterCount: Int): Unit = {
    val top = new AdaDeltaExpr(lr, rho.toFloat, isInplace = true)
    val fop = new DefaultTernary(lr, isInplace = true)
    ExecuteUtils.executeScalar(grad, new ScalarExpr(alpha = 1.0f / numSample, top = TOperation.Mul, isInplace = true))
    val delta = ExecuteUtils.executeTernary(grad, sumDelta2, sumGrad2, model.getIndexFlag, top, fop)

    if (l2Reg != null) {
      val localMap = l2Reg.map { case (name: String, lam: Double) => name -> (1.0 - lr * lam) }
      val oneMap = localParams.map { case (name: String, _) => name -> 1.0 }.toMap
      OptUtils.ilinear(localParams, localMap, delta, oneMap)
    } else {
      OptUtils.iaxpy(localParams, delta, 1.0)
    }
  }

  override def initialLocal(model: OptModel, local: util.HashMap[String, TUpdate], global: util.HashMap[String, TUpdate]): Unit = {
    if (sumGrad2 == null && l2Reg != null) {
      sumGrad2 = OptUtils.emptyLike(global)
      sumDelta2 = OptUtils.emptyLike(global)
    } else if (sumGrad2 == null && l2Reg == null) {
      sumGrad2 = model.getZeroParams
      sumDelta2 = model.getZeroParams
    } else {
      val delta = OptUtils.axpy(local, global, -0.1)
      sumGrad2 = ExecuteUtils.executeScalar(delta, new ScalarExpr(2.0f, TOperation.Pow, true))
      OptUtils.clear(sumDelta2)
    }
  }

  override def psfHook(model: OptModel, numSample: Int): Unit = {
    val thresh = if (l1Reg != null) {
      model.getPSModels.map { case (name: String, _) =>
        if (l2Reg != null) {
          name -> lr * l1Reg(name) / (1.0 + lr * l2Reg(name))
        } else {
          name -> lr * l1Reg(name)
        }
      }
    } else null

    model.psfHook(thresh)
  }
}
