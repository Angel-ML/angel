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

package com.tencent.angel.ml.optimizer2.lossfuncs

class HingeLoss(val beta: Double = 0.999) {
  assert(beta >= 0 && beta <= 1)
  private val a: Double = 1.0 / Math.pow(1.0 - beta, 2)
  private val b: Double = 2.0 / (1.0 - beta)

  def apply(ymodel: Double, yture: Double): Double = {
    val pre = ymodel * yture
    if (pre <= beta) {
      pre - 1.0
    } else if (pre >= 1) {
      0.0
    } else {
      a * Math.pow(pre - 1.0, 3) + b * Math.pow(pre - 1.0, 2)
    }
  }

  def grad(ymodel: Double, yture: Double): Double = {
    val pre = ymodel * yture
    if (pre <= beta) {
      -1.0
    } else if (pre >= 1) {
      0.0
    } else {
      3 * a * Math.pow(pre - 1.0, 2) + 2 * b * (pre - 1.0)
    }
  }
}
