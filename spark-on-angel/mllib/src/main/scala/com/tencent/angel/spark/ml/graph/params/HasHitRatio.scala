package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{DoubleParam, Params}

trait HasHitRatio extends Params {
  final val hitRatio = new DoubleParam(this, "HitRatio", "Hit Ratio")

  final def getHitRatio: Double = $(hitRatio)

  final def setHitRatio(hr: Double): this.type = set(hitRatio, hr)
}
