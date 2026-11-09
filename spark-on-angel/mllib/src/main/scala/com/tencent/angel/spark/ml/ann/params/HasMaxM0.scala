package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasMaxM0 extends Params {
  /**
    * Param for ef.
    *
    * @group param
    */
  final val maxM0 = new IntParam(this, "maxM0", "param for maxM0")
  
  /** @group getParam */
  final def getMaxM0: Int = $(maxM0)
  
  setDefault(maxM0, 10)
  
  /** @group setParam */
  final def setMaxM0(m: Int): this.type = set(maxM0, m)
}