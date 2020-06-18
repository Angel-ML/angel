package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{FloatParam, Params}

trait HasStepSize extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val stepSize = new FloatParam(this, "stepSize", "stepSize")

  /** @group getParam */
  final def getStepSize: Float = $(stepSize)

  setDefault(stepSize, 0.025f)

  /** @group setParam */
  final def setStepSize(lr: Float): this.type = set(stepSize, lr)
}
