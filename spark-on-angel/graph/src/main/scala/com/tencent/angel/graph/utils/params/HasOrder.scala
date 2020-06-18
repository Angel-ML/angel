package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasOrder extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val order = new IntParam(this, "order", "order")

  /** @group getParam */
  final def getOrder: Int = $(order)

  setDefault(order, 2)

  /** @group setParam */
  final def setOrder(orderValue: Int): this.type = set(order, orderValue)
}
