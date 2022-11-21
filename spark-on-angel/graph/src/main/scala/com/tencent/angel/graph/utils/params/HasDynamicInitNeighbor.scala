package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasDynamicInitNeighbor extends Params{
  /**
   * Param for dynamic initialize neighbors.
   *
   * @group param
   */
  final val dynamicInitNeighbor = new BooleanParam(this, "dynamicInitNeighbor", "whether to dynamically initialize neighbors")

  final def getDynamicInitNeighbor : Boolean = $(dynamicInitNeighbor)

  setDefault(dynamicInitNeighbor, false)

  final def setDynamicInitNeighbor (bool: Boolean): this.type = set(dynamicInitNeighbor, bool)
}
