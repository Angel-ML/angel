package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasDistanceFunction extends Params {
  /**
    * Param for name of distance function.
    *
    * @group param
    */
  final val disFunc = new Param[String](this, "disFunc", "name for distance function")
  
  /** @group getParam */
  final def getDisFunc: String = $(disFunc)
  
  setDefault(disFunc, "l2")
  
  /** @group setParam */
  final def setDisFunc(name: String): this.type = set(disFunc, name)
}