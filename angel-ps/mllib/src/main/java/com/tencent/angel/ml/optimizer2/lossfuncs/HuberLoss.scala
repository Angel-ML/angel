package com.tencent.angel.ml.optimizer2.lossfuncs

class HuberLoss(val alpha:Double=0.999) {
  def apply(ymodel:Double, yture:Double):Double = {
    val delta = Math.abs(ymodel - yture)
    if (delta < alpha) {
      0.5 * delta * delta
    } else {
      alpha * (delta - 0.5 * alpha)
    }
  }

  def grad(ymodel:Double, yture:Double):Double = {
    val delta = ymodel - yture
    if (Math.abs(ymodel - yture) < alpha) {
      delta
    } else {
      Math.sin(delta) * alpha
    }
  }
}
