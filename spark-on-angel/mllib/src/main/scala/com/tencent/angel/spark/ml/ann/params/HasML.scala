package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{DoubleParam, Params}

trait HasML extends Params {
  /**
    * Param for ef.
    *
    * @group param
    */
  final val mL = new DoubleParam(this, "mL", "param for mL")
  
  /** @group getParam */
  final def getML: Double = $(mL)
  
  setDefault(mL, 1.0)
  
  /** @group setParam */
  final def setML(m: Double): this.type = set(mL, m)
}