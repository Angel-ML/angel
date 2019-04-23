package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasIsWeighted extends Params {
  /**
    * Param for isWeighted.
    *
    * @group param
    */
  final val isWeighted = new BooleanParam(this, "isWeighted", "is weighted graph or not")

  /** @group getParam */
  final def getIsWeighted: Boolean = $(isWeighted)

  setDefault(isWeighted, false)

  final def setIsWeighted(bool: Boolean): this.type = set(isWeighted, bool)
}
