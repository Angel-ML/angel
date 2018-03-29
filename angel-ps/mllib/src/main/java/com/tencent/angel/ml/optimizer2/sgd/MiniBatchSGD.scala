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
import com.tencent.angel.ml.optimizer2.utils.OptUtils
import com.tencent.angel.ml.optimizer2.{OptModel, Optimizer}

import scala.collection.JavaConversions._

class MiniBatchSGD(batchSize: Int, numUpdatePerEpoch: Int, lr: Double,
                   val l1Reg: Map[String, Double] = null, val l2Reg: Map[String, Double] = null)
  extends Optimizer(batchSize, numUpdatePerEpoch, lr) {

  override def updateLocal(model: OptModel, numSample: Int, iterCount: Int): Unit = {
    if (l1Reg != null && l2Reg != null) {
      ElasticNetUpdate(model, numSample)
    } else if (l1Reg == null && l2Reg != null) {
      L2RegUpdate(model, numSample)
    } else {
      OptUtils.iaxpy(localParams, grad, -lr / numSample)
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

  private def L2RegUpdate(model: OptModel, numSample: Int): Unit = {
    val alpha = l2Reg.map { case (name, eta) =>
      if (model.getIndexFlag.get(name)) {
        name -> (1.0 - lr * eta)
      } else {
        name -> 1.0
      }
    }
    val beta = l2Reg.map { case (name, _) => name -> -lr / numSample }

    OptUtils.ilinear(localParams, alpha, grad, beta)
  }

  private def ElasticNetUpdate(model: OptModel, numSample: Int): Unit = {
    val alpha = l2Reg.map { case (name, eta) =>
      if (model.getIndexFlag.get(name)) {
        name -> 1.0 / (1.0 + lr * eta)
      } else {
        name -> 1.0
      }
    }

    val beta = l2Reg.map { case (name, eta) =>
      val gc = if (model.getIndexFlag.get(name)) {
        -lr / numSample / (1.0 + lr * eta)
      } else {
        -lr / numSample
      }
      name -> gc
    }
    OptUtils.ilinear(localParams, alpha, grad, beta)
  }

  override def initialLocal(model: OptModel, local: util.HashMap[String, TUpdate], global: util.HashMap[String, TUpdate]): Unit = {}
}
