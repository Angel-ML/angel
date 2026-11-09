package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasIsFastApproximation extends Params {
  /**
    * Param for fast approximation.
    *
    * @group param
    */
  final val fastApproximation = new BooleanParam(this, "fastApproximation", "name for fast approximation")
  
  /** @group getParam */
  final def getFastApproximation: Boolean = $(fastApproximation)
  
  setDefault(fastApproximation, false)
  
  /** @group setParam */
  final def setFastApproximation(flag: Boolean): this.type = set(fastApproximation, flag)
}