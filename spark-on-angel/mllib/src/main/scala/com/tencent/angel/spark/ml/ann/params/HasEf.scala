package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasEf extends Params {
  /**
    * Param for ef.
    *
    * @group param
    */
  final val ef = new IntParam(this, "ef", "param for ef")
  
  /** @group getParam */
  final def getEf: Int = $(ef)
  
  setDefault(ef, 10)
  
  /** @group setParam */
  final def setEf(m: Int): this.type = set(ef, m)
}