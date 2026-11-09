package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasItemIdx extends Params {
  /**
    * Param for item index in the input table.
    *
    * @group param
    */
  final val itemIdx = new IntParam(this, "itemIdx", "param for item index in the input table")
  
  /** @group getParam */
  final def getItemIdx: Int = $(itemIdx)
  
  setDefault(itemIdx, 0)
  
  /** @group setParam */
  final def setItemIdx(m: Int): this.type = set(itemIdx, m)
}