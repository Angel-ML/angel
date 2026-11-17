package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasVectorElementType extends Params {
  /**
    * Param for type of elements in vector.
    *
    * @group param
    */
  final val vecEleType = new Param[String](this, "vecEleType", "type for element in vector")
  
  /** @group getParam */
  final def getVecEleType: String = $(vecEleType)
  
  setDefault(vecEleType, "float")
  
  /** @group setParam */
  final def setVecEleType(name: String): this.type = set(vecEleType, name)
}