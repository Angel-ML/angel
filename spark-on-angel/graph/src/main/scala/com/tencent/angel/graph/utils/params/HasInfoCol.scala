package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasInfoCol extends Params {
  /**
   * Param for infoCol.
   *
   * @group param
   */
  final val infoCol = new Param[String](this, "infoCol", "name for info column ")

  /** @group getParam */
  final def getInfoCol: String = $(infoCol)

  final def setInfoCol(name: String): this.type = set(infoCol, name)

  setDefault(infoCol, "info")
}
