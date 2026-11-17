package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasInputDim extends Params {
  /**
    * Param for input dim.
    *
    * @group param
    */
  final val inputDim = new IntParam(this, "InputDim", "param for input dim")
  
  /** @group getParam */
  final def getInputDim: Int = $(inputDim)
  
  setDefault(inputDim, 10)
  
  /** @group setParam */
  final def setInputDim(m: Int): this.type = set(inputDim, m)
}