package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasM extends Params {
  /**
    * Param for M.
    *
    * @group param
    */
  final val M = new IntParam(this, "M", "param for M")
  
  /** @group getParam */
  final def getM: Int = $(M)
  
  setDefault(M, 10)
  
  /** @group setParam */
  final def setM(m: Int): this.type = set(M, m)
}