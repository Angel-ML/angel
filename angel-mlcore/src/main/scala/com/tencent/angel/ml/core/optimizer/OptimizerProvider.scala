package com.tencent.angel.ml.core.optimizer

import org.json4s.JsonAST.{JObject, JValue}

trait OptimizerProvider {
  def optFromJson(json: JValue): Optimizer

  def defaultOptJson(): JObject
}