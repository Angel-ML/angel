package com.tencent.angel.ml.optimizer2.lossfuncs

object SquareHingeLoss {
  def apply(ymodel:Double, ytrue:Double): Double = {
    if (ymodel * ytrue > 1.0) { 0.0 } else {
      0.5 * ymodel * ymodel
    }
  }

  def grad(ymodel:Double, ytrue:Double): Double = {
    if (ymodel * ytrue > 1.0) { 0.0 } else {
      ytrue * ymodel
    }
  }
}
