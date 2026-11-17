package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasItemSep extends Params {
  /**
    * Param for sep between item and vec.
    *
    * @group param
    */
  final val itemSep = new Param[String](this, "itemSep", "sep between item and vec")
  
  /** @group getParam */
  final def getItemSep: String = $(itemSep)
  
  setDefault(itemSep, " ")
  
  /** @group setParam */
  final def setItemSep(name: String): this.type = set(itemSep, name)
}