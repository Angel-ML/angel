package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasLabelCol extends Params {
  /**
   * Param for infoCol.
   *
   * @group param
   */
  final val labelCol = new Param[String](this, "labelCol", "name for label column ")

  /** @group getParam */
  final def getLabelCol: String = $(labelCol)

  final def setLabelCol(name: String): this.type = set(labelCol, name)

  setDefault(labelCol, "label")
}
