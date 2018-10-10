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


package com.tencent.angel.ml.auto.acquisition

import com.tencent.angel.ml.auto.surrogate.BaseSurrogate
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntDoubleVectorStorage, IntFloatDenseVectorStorage}
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, Vector}
import org.apache.commons.math3.distribution.NormalDistribution

/**
  * Expected improvement.
  * @param surrogate
  * @param par : Controls the balance between exploration and exploitation of the acquisition function, default=0.0
  *
 */
class EI(override val surrogate: BaseSurrogate, val par: Float) extends BaseAcquisition(surrogate) {

  override def compute(X: Vector, derivative: Boolean = false): (Float, Vector) = {
    val pred = surrogate.predict(X) // (mean, variance)

    // Use the best seen observation as incumbent
    val eta: Float = surrogate.curBest._2

    val s: Float = Math.sqrt(pred._2).toFloat

    if (s == 0) {
      // if std is zero, we have observed x on all instances
      // using a RF, std should be never exactly 0.0
      (0.0f, new IntFloatVector(X.dim().toInt, new IntFloatDenseVectorStorage()))
    } else {
      val z = (eta - pred._1 - par) / s
      val norm: NormalDistribution  = new NormalDistribution
      val f = s * (z * norm.cumulativeProbability(z) + norm.density(z))
      (f.toFloat, new IntFloatVector(X.dim().toInt, new IntFloatDenseVectorStorage()))
    }
  }
}