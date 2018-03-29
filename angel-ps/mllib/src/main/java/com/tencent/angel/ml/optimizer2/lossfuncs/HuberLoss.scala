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

class HuberLoss(val alpha: Double = 0.999) {
  def apply(ymodel: Double, yture: Double): Double = {
    val delta = Math.abs(ymodel - yture)
    if (delta < alpha) {
      0.5 * delta * delta
    } else {
      alpha * (delta - 0.5 * alpha)
    }
  }

  def grad(ymodel: Double, yture: Double): Double = {
    val delta = ymodel - yture
    if (Math.abs(ymodel - yture) < alpha) {
      delta
    } else {
      Math.sin(delta) * alpha
    }
  }
}
