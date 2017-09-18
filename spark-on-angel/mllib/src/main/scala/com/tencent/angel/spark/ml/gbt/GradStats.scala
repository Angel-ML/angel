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
 */
package com.tencent.angel.spark.ml.gbt

/**
 * Description: gradient info of reg tree
 */
class GradStats(var sumGrad: Double, var sumHess: Double) {

  private def clear: Unit = {
    this.sumGrad = 0.0
    this.sumHess = 0.0
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

  /**
   * add statistics to the data.
   *
   * @param b Grad statistic to add
   */
  def add(b: GradStats) {
    this.add(b.sumGrad, b.sumHess)
  }

  /**
   * set current value to a - b.
   *
   * @param a the grad being substracted
   * @param b the grad to substract
   */
  def setSubstract(a: GradStats, b: GradStats) {
    sumGrad = a.sumGrad - b.sumGrad
    sumHess = a.sumHess - b.sumHess
  }
}
