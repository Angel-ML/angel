package com.tencent.angel.spark.ml.params

import org.apache.spark.ml.param.{Params, StringArrayParam}

trait HasOutputCols extends Params {

  /**
   * Param for output column names.
   * @group param
   */
  final val outputCols: StringArrayParam =
    new StringArrayParam(this, "outputCols", "output column names")

  /** @group getParam */
  final def getOutputCols: Array[String] = $(outputCols)
}
