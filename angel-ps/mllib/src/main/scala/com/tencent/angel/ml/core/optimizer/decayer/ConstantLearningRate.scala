package com.tencent.angel.ml.core.optimizer.decayer

class ConstantLearningRate(eta: Double) extends StepSizeScheduler {

  override def next(): Double = {
    eta
  }

}
