package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasPullSizeInMB extends Params {
  /**
    * Param for batch size.
    *
    * @group param
    */
  final val pullSizeInMB = new IntParam(this, "pullSizeInMB", "pullSizeInMB")

  /** @group getParam */
  final def getPullSize: Int = $(pullSizeInMB)

  /** @group setParam */
  final def setPullSize(size: Int): this.type = set(pullSizeInMB, size)
}
