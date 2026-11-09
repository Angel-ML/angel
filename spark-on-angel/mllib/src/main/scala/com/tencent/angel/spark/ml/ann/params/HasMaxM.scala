package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasMaxM extends Params {
  /**
    * Param for ef.
    *
    * @group param
    */
  final val maxM = new IntParam(this, "maxM", "param for maxM")
  
  /** @group getParam */
  final def getMaxM: Int = $(maxM)
  
  setDefault(maxM, 10)
  
  /** @group setParam */
  final def setMaxM(m: Int): this.type = set(maxM, m)
}