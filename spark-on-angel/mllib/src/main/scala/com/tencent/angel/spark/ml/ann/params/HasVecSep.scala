package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasVecSep extends Params {
  /**
    * Param for sep of vec.
    *
    * @group param
    */
  final val vecSep = new Param[String](this, "vecSep", "sep of vec")
  
  /** @group getParam */
  final def getVecSep: String = $(vecSep)
  
  setDefault(vecSep, " ")
  
  /** @group setParam */
  final def setVecSep(name: String): this.type = set(vecSep, name)
}