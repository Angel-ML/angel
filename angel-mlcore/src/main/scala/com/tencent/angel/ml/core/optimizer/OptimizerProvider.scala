package com.tencent.angel.ml.core.optimizer

trait OptimizerProvider {
  def optFromJson(jsonStr: String): Optimizer

  def setRegParams[T <: Optimizer](opt: T, jastStr: String): T
}