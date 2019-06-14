package com.tencent.angel.ml.core.optimizer

import com.tencent.angel.ml.core.conf.SharedConf

trait OptimizerProvider {
  val conf: SharedConf = SharedConf.get()

  def optFromJson(jsonStr: String): Optimizer

  def setRegParams[T <: Optimizer](opt: T, jastStr: String): T
}