package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasDegreeBinSize extends Params {
  final val degreeBinSize = new IntParam(this, "degreeBinSize", "number of degree bin")

  final def getDegreeBinSize: Int = $(degreeBinSize)

  final def setDegreeBinSize(num: Int): this.type = set(degreeBinSize, num)
}
