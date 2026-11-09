package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasVecIdx extends Params {
  /**
    * Param for vec index in the input table.
    *
    * @group param
    */
  final val vecIdx = new IntParam(this, "vecIdx", "param for vec index in the input table")
  
  /** @group getParam */
  final def getVecIdx: Int = $(vecIdx)
  
  setDefault(vecIdx, 0)
  
  /** @group setParam */
  final def setVecIdx(m: Int): this.type = set(vecIdx, m)
}