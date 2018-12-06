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


package com.tencent.angel.ml.core

class GraphPredictResult(val sid: String, val pred: Double, val proba: Double, val label: Double) {

  override def getText: String = {
    sid + separator +
      format.format(pred) + separator +
      format.format(proba) + separator +
      format.format(label)
  }
}


object GraphPredictResult{
  def apply(sid: String, pred: Double, proba: Double, label: Double): GraphPredictResult = {
    new GraphPredictResult(sid, pred, proba, label)
  }
}

class SoftmaxPredictResult(sid: String, pred: Double, proba: Double, val trueProba: Double, label: Double)
  extends GraphPredictResult(sid, pred, proba, label) {

  override def getText: String = {
    sid + separator +
      format.format(pred) + separator +
      format.format(proba) + separator +
      format.format(trueProba) + separator +
      format.format(label)
  }
}

object SoftmaxPredictResult {
  def apply(sid: String, pred: Double, proba: Double, trueProba: Double, label: Double): SoftmaxPredictResult = {
    new SoftmaxPredictResult(sid, pred, proba, trueProba, label)
  }
}