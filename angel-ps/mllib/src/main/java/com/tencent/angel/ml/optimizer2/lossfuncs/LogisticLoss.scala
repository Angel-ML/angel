package com.tencent.angel.ml.optimizer2.lossfuncs

object LogisticLoss {
  private val threshold = Math.log(Double.MaxValue)

  def apply(ymodel:Double, yture:Double):Double = {
    val temp = - yture * ymodel
    if (temp > threshold) {
      temp
    } else {
      Math.log1p(Math.exp(- yture * ymodel))
    }
  }

  def grad(ymodel:Double, yture:Double):Double = {
    val temp = - yture * ymodel
    if (temp > threshold) {
      0.0
    } else {
      - yture /(1.0 + Math.exp(yture * ymodel))
    }

  }
}
