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

// https://web.stanford.edu/~tdozat/files/TDozat-CS229-Paper.pdf
// https://zh.gluon.ai/chapter_optimization/gd-sgd-gluon.html

class Adam(batchSize: Int, numUpdatePerEpoch: Int, lr: Double,
           val rho: Double, val phi: Double, val l2Reg: Map[String, Double] = null)
  extends Optimizer(batchSize, numUpdatePerEpoch, lr) {
  private var sumGrad1: util.HashMap[String, TUpdate] = _
  private var sumGrad2: util.HashMap[String, TUpdate] = _

  override def updateLocal(model: OptModel, numSample: Int, iterCount: Int): Unit = {
    val gradWithReg = if (l2Reg != null) {
      val oneMap = localParams.map { case (name: String, _) => name -> 1.0 / numSample }.toMap
      OptUtils.linear(localParams, l2Reg, grad, oneMap)
    } else {
      ExecuteUtils.executeScalar(grad, new ScalarExpr(alpha = 1.0f / numSample, top = TOperation.Mul, isInplace = true))
    }

    val top = new AdamExpr(iterCount, lr, rho.toFloat, phi.toFloat, isInplace = true)
    val fop = new DefaultTernary(lr, isInplace = true)
    val delta = ExecuteUtils.executeTernary(gradWithReg, sumGrad1, sumGrad2, model.getIndexFlag, top, fop)
    OptUtils.iaxpy(localParams, delta, 1.0)
  }

  override def initialLocal(model: OptModel, local: util.HashMap[String, TUpdate], global: util.HashMap[String, TUpdate]): Unit = {
    if (sumGrad2 == null && l2Reg != null) {
      sumGrad2 = OptUtils.emptyLike(global)
      sumGrad1 = OptUtils.emptyLike(global)
    } else if (sumGrad2 == null && l2Reg == null) {
      sumGrad2 = model.getZeroParams
      sumGrad1 = model.getZeroParams
    } else {
      sumGrad1 = OptUtils.axpy(global, local, -1.0)
      sumGrad2 = ExecuteUtils.executeScalar(sumGrad1, new ScalarExpr(2.0f, TOperation.Pow, false))
    }
  }

}
