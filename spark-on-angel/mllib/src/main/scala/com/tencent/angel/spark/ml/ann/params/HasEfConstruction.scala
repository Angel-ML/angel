package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasEfConstruction extends Params {
  /**
    * Param for ef.
    *
    * @group param
    */
  final val efConstruction = new IntParam(this, "efConstruction", "param for efConstruction")
  
  /** @group getParam */
  final def getEfConstruction: Int = $(efConstruction)
  
  setDefault(efConstruction, 10)
  
  /** @group setParam */
  final def setEfConstruction(m: Int): this.type = set(efConstruction, m)
}