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

trait Loss extends Serializable {

  def transPred(pred: Float): Float

  def firstGradient(label: Float, pred: Float): Float

  def secondGradient(label: Float, pred: Float): Float

  def evalMetric: String
}


class LeastSquareLoss extends Loss {

  def value(label: Float, pred: Float): Float = {
    (label - pred) * (label - pred)
  }

  def transPred(pred: Float): Float = {
    pred
  }

  def firstGradient(label: Float, pred: Float): Float = {
      pred - label
  }

  def secondGradient(label: Float, pred: Float): Float = {
    1.0f
  }

  def evalMetric: String = "RMSE"
}

class LogisticLoss extends Loss {

  def value(label: Float, pred: Float): Float = {
    (label * math.log(pred) + (1 - label) * math.log(1 - pred)).toFloat
  }

  def transPred(pred: Float): Float = {
    1.0f / (1 + math.exp(-1 * pred).toFloat)
  }

  override def firstGradient(label: Float, pred: Float): Float = {
    pred -  label
  }

  override def secondGradient(label: Float, pred: Float): Float = {
    val eps = 1e-16f
    Math.max(pred * (1 - pred), eps)
  }

  def evalMetric: String = "AUC"
}

