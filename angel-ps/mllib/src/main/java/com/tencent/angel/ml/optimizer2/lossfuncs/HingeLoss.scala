package com.tencent.angel.ml.optimizer2.lossfuncs

class HingeLoss(val beta:Double=0.999) {
  assert(beta >=0 && beta <=1)
  private val a: Double = 1.0 / Math.pow(1.0 - beta, 2)
  private val b: Double = 2.0 / (1.0 - beta)

  def apply(ymodel:Double, yture:Double):Double = {
    val pre = ymodel * yture
    if (pre <= beta) {
      pre - 1.0
    } else if (pre >= 1) {
      0.0
    } else {
      a * Math.pow(pre -1.0, 3) + b * Math.pow(pre -1.0, 2)
    }
  }

  def grad(ymodel:Double, yture:Double):Double = {
    val pre = ymodel * yture
    if (pre <= beta) {
      - 1.0
    } else if (pre >= 1) {
      0.0
    } else {
      3 * a * Math.pow(pre -1.0, 2) + 2 * b * (pre -1.0)
    }
  }
}
