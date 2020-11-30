package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutput extends Params {
  final val output = new Param[String](this, "output", "output")

  final def getOutput: String = $(output)

  setDefault(output, null)

  final def setOutput(in: String): this.type = set(output, in)
}
