package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputNodeIdCol extends Params {
  /**
    * Param for name of output node id.
    *
    * @group param
    */
  final val outputNodeIdCol = new Param[String](this, "outputNodeIdCol", "name for output node id column")

  /** @group getParam */
  final def getOutputNodeIdCol: String = $(outputNodeIdCol)

  setDefault(outputNodeIdCol, "node")

  /** @group setParam */
  def setOutputNodeIdCol(name: String): this.type = set(outputNodeIdCol, name)
}
