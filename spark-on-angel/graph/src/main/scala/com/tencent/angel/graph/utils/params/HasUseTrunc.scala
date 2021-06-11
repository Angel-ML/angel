package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}


trait HasUseTrunc extends Params{
  /**
   * Param for using Truncation.
   *
   * @group param
   */
  final val useTrunc = new BooleanParam(this, "useTrunc", "need trunc edge or not")

  final def getUseTrunc : Boolean = $(useTrunc)

  final def setUseTrunc (bool: Boolean): this.type = set(useTrunc, bool)
}