package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasNeedRemapping extends Params {
  final val remapping = new BooleanParam(this, "remapping", "remapping")

  final def getRemapping: Boolean = $(remapping)

  setDefault(remapping, false)

  final def setRemapping(flag: Boolean): this.type = set(remapping, flag)
}
