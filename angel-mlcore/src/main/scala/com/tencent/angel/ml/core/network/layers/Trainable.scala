package com.tencent.angel.ml.core.network.layers

import com.tencent.angel.ml.core.optimizer.Optimizer

trait Trainable {
  def optimizer: Optimizer
}