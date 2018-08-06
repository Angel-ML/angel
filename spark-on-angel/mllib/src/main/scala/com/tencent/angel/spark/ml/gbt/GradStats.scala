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
package com.tencent.angel.spark.ml.gbt

/**
 * Description: gradient info of reg tree
 */
class GradStats(
    var sumGrad: Double = 0.0,
    var sumHess: Double = 0.0) extends Serializable {

  def clear() {
    this.sumGrad = 0.0f
    this.sumHess = 0.0f
  }


  def update(sumGrad: Double, sumHess: Double) {
    this.sumGrad = sumGrad
    this.sumHess = sumHess
  }

  /**
   * add statistics to the data.
   *
   * @param grad the grad to add
   * @param hess the hess to add
   */
  def add(grad: Double, hess: Double) {
    this.sumGrad += grad
    this.sumHess += hess
  }

  def add(b: GradStats) {
    this.add(b.sumGrad, b.sumHess)
  }

  def substract(a: GradStats, b: GradStats) {
    sumGrad = a.sumGrad - b.sumGrad
    sumHess = a.sumHess - b.sumHess
  }

  def calcWeight(param: GBTreeParam): Double = {
    if (sumHess <= param.minChildWeight) return 0.0f
    var dw: Double = 0.0
    if (param.regAlpha == 0.0f) {
      dw = -sumGrad / (sumHess + param.regLambda)
    } else {
      dw = -1 * shrinkage(sumGrad, param.regAlpha) / (sumHess + param.regLambda)
    }
    if (param.maxDeltaStep > 0.0f) {
      if (dw > param.maxDeltaStep) dw = param.maxDeltaStep
      if (dw < -param.maxDeltaStep) dw = -param.maxDeltaStep
    }
    dw
  }


  def calcLoss(param: GBTreeParam): Double = {
    if (sumHess <= param.minChildWeight) return 0.0

    var loss = 0.0
    if (param.maxDeltaStep == 0.0f) {
      if (param.regAlpha == 0.0f) {
        loss = (sumGrad / (sumHess + param.regLambda)) * sumGrad
      } else {
        val temp = shrinkage(sumGrad, param.regAlpha)
        loss = temp * temp / (sumHess + param.regLambda)
      }
    } else {
      val w = calcWeight(param)
      val ret = sumGrad * w + 0.5f * (sumHess + param.regLambda) * w * w
      if (param.regAlpha == 0.0f) {
        loss = -2.0 * ret
      } else {
        loss = -2.0 * (ret + param.regAlpha * Math.abs(w))
      }
    }
    loss
  }

  /**
   * The corresponding proximal operator for the L1 norm is the soft-threshold
   * function. That is, each value is shrunk towards 0 by kappa value.
   *
   * if x < kappa or x > -kappa, return 0
   * if x > kappa, return x - kappa
   * if x < -kappa, return x + kappa
   */
  private def shrinkage(x: Double, kappa: Double): Double = {
    math.max(0, x - kappa) - math.max(0, -x - kappa)
  }

  override def toString: String = {
    s"GradStats(sumGrad: $sumGrad sumHess: $sumHess)"
  }

}
