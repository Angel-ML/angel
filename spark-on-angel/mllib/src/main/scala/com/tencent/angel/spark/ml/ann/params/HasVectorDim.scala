package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasVectorDim extends Params {
  /**
    * Param for dim of vectors.
    *
    * @group param
    */
  final val vectorDim = new IntParam(this, "vectorDim", "param for dim of vectors")
  
  /** @group getParam */
  final def getVectorDim: Int = $(vectorDim)
  
  setDefault(vectorDim, 10)
  
  /** @group setParam */
  final def setVectorDim(m: Int): this.type = set(vectorDim, m)
}
