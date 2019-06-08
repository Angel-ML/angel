package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasDebugMode extends Params {

  final val debugMode = new BooleanParam(this, "debugMode", "debugMode")

  final def setDebugMode(mode: Boolean): this.type = set(debugMode, mode)

  final def getDebugMode: Boolean = $(debugMode)
}
