package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasWalkLength extends Params {
  /**
    * Param for walkLength.
    *
    * @group param
    */
  final val walkLength = new IntParam(this, "walkLength", "the length of walk path")

  /** @group getParam */
  final def getWalkLength: Int = $(walkLength)

  final def setWalkLength(num: Int): this.type = set(walkLength, num)

}
