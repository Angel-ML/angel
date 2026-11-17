package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasMaxItemCount extends Params {
  /**
    * Param for maxItemCount.
    *
    * @group param
    */
  final val maxItemCount = new IntParam(this, "maxItemCount", "param for maxItemCount")
  
  /** @group getParam */
  final def getMaxItemcCount: Int = $(maxItemCount)
  
  setDefault(maxItemCount, 10)
  
  /** @group setParam */
  final def setMaxItemCount(m: Int): this.type = set(maxItemCount, m)
}