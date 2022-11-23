package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasCounterCol extends Params {
  /**
   * Param for name of counter id.
   *
   * @group param
   */
  final val counterCol = new Param[String](this, "counterCol", "name for counter column")

  /** @group getParam */
  final def getCounterCol: String = $(counterCol)

  setDefault(counterCol, "counter")

  /** @group setParam */
  def setCounterCol(name: String): this.type = set(counterCol, name)
}
