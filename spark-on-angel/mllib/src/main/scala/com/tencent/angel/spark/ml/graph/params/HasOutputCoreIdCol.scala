package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputCoreIdCol extends Params {

  /**
    * Param for name of output community id.
    *
    * @group param
    */
  final val outputCoreIdCol = new Param[String](this, "outputCoreIdCol", "name for output coreness column")

  /** @group getParam */
  final def getOutputCoreIdCol: String = $(outputCoreIdCol)

  setDefault(outputCoreIdCol, "coreness")

  /** @group setParam */
  final def setOutputCoreIdCol(name: String): this.type = set(outputCoreIdCol, name)

}
