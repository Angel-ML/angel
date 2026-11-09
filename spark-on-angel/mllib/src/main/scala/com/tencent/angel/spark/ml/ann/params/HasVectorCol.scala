package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasVectorCol extends Params {
  /**
    * Param for name of vectors.
    *
    * @group param
    */
  final val vectorCol = new Param[String](this, "vectorCol", "name for vector column")
  
  /** @group getParam */
  final def getVectorCol: String = $(vectorCol)
  
  setDefault(vectorCol, "vector")
  
  /** @group setParam */
  final def setVectorCol(name: String): this.type = set(vectorCol, name)
}