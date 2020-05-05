package com.tencent.angel.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasComputeLcc extends Params {

  final val computeLcc = new BooleanParam(this, "computeLcc", "computeLcc")

  final def setComputeLcc(enable: Boolean): this.type = set(computeLcc, enable)

  final def getComputeLcc: Boolean = $(computeLcc)
}
