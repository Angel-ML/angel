package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasExecMode extends Params {
  
  final val execMode = new Param[String](this, "execMode", "execMode")
  
  final def getExecMode: String = $(execMode)
  
  setDefault(execMode, null)
  
  final def setExecMode(in: String): this.type = set(execMode, in)
  
}
