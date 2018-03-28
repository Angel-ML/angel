package com.tencent.angel.ml.optimizer2.lossfuncs

object ExpLoss {
  def apply(ymodel:Double, yture:Double):Double = {
    Math.exp(- yture * ymodel)
  }

  def grad(ymodel:Double, yture:Double):Double = {
    - yture * Math.exp(- yture * ymodel)
  }
}
