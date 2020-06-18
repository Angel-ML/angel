package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasSubSample extends Params {
  final val subSample = new BooleanParam(this, "subSample", "subSample")

  final def getSubSample: Boolean = $(subSample)

  setDefault(subSample, false)

  final def setSubSample(flag: Boolean): this.type = set(subSample, flag)
}
