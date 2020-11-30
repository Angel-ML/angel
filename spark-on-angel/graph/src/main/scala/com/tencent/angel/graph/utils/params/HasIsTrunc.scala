package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}


trait HasIsTrunc extends Params {
  /**
    * Param for isCompressed.
    *
    * @group param
    */
  final val isTrunc = new BooleanParam(this, "isTrunc", "need trunc edge or not")

  final def getIsTrunc: Boolean = $(isTrunc)

  final def setIsTrunc(bool: Boolean): this.type = set(isTrunc, bool)
}