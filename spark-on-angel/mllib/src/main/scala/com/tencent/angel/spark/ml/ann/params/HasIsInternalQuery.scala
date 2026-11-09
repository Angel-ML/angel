package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasIsInternalQuery extends Params {
  /**
    * Param for internal query flag.
    *
    * @group param
    */
  final val isInternal = new BooleanParam(this, "isInternal", "name for internal query flag")
  
  /** @group getParam */
  final def getIsInternal: Boolean = $(isInternal)
  
  setDefault(isInternal, false)
  
  /** @group setParam */
  final def setIsInternal(flag: Boolean): this.type = set(isInternal, flag)
}