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

package com.tencent.angel.spark.ml.common


import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, DenseVector}

/**
 * Class used to compute the gradient for a loss function, given a single data point.
 */
trait Gradient extends Serializable {
  def compute(data: OneHotVector, label: Double, weights: DenseVector): Double
  def compute(
    data: OneHotVector,
    label: Double,
    weights: DenseVector,
    gradient: DenseVector): Double
}

/**
 * Compute gradient and loss for a binary logistic loss function. Only Support binary
 * classification, so `numClasses` is 2.
 *
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression. By default, it is binary logistic regression
 *                   so numClasses will be set to 2.
 */
class LogisticGradient(numClasses: Int = 2) extends Gradient {

  def this() = this(2)

  override def compute(data: OneHotVector, label: Double, weights: DenseVector): Double = {
    numClasses match {
      case 2 =>
        val margin = -1.0 * BLAS.dot(data, new DenseVector(weights.toArray))
        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          log1pExp(margin)
        } else {
          log1pExp(margin) - margin
        }
      case _ =>
        throw new Exception("Logistic can not support multiClass")
    }
  }

  override def compute (data: OneHotVector,
                        label: Double,
                        weights: DenseVector,
                        cumGradient: DenseVector): Double = {
    require(weights.length == cumGradient.length)
    numClasses match {
      case 2 =>
        val margin = -1.0 * BLAS.dot(data, new DenseVector(weights.toArray))
        val multiplier = (1.0 / (1.0 + math.exp(margin))) - label
        BLAS.axpy(multiplier, data, new DenseVector(cumGradient.toArray))
        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          log1pExp(margin)
        } else {
          log1pExp(margin) - margin
        }
      case _ =>
        throw new Exception("Logistic can not support multiClass")
    }
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}



