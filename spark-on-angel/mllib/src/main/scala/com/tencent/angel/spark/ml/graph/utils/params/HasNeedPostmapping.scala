package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasNeedPostmapping extends Params {
  final val postMapping = new BooleanParam(this, "postMapping", "postMapping")

  final def getPostMapping: Boolean = $(postMapping)

  setDefault(postMapping, false)

  final def setPostMapping(flag: Boolean): this.type = set(postMapping, flag)
}
