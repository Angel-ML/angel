package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasEpochNum extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val epochNum = new IntParam(this, "epochNum", "epochNum")

  /** @group getParam */
  final def getEpochNum: Int = $(epochNum)

  setDefault(epochNum, 5)

  /** @group setParam */
  final def setEpochNum(num: Int): this.type = set(epochNum, num)
}
