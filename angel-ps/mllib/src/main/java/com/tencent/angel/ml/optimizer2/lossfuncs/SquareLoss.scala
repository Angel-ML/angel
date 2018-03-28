package com.tencent.angel.ml.optimizer2.lossfuncs

object SquareLoss {
  def apply(ymodel:Double, ytrue:Double): Double = {
    Math.pow(ymodel - ytrue, 2)
  }

  def grad(ymodel:Double, ytrue:Double): Double = {
    ymodel - ytrue
  }
}
