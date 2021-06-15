package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasSetCheckPoint extends Params {
  /**
   * Param for setCheckPoint.
   *
   * @group param
   */
  final val setCheckPoint = new BooleanParam(this, "setCheckPoint", "set checkpoint or not")

  final def getSetCheckPoint : Boolean = $(setCheckPoint)

  setDefault(setCheckPoint, false)

  final def setCheckPoint (bool: Boolean): this.type = set(setCheckPoint, bool)
}
