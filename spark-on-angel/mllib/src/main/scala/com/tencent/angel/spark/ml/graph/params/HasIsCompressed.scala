package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasIsCompressed extends Params {
  /**
    * Param for isCompressed.
    *
    * @group param
    */
  final val isCompressed = new BooleanParam(this, "isCompressed", "is compressed edge or not")

  final def getIsCompressed : Boolean = $(isCompressed)

  setDefault(isCompressed, false)

  final def setIsCompressed (bool: Boolean): this.type = set(isCompressed, bool)
}
