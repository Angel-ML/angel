package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasItemIdCol extends Params {
  /**
    * Param for name of item id.
    *
    * @group param
    */
  final val itemIdCol = new Param[String](this, "itemIdCol", "name for item column")
  
  /** @group getParam */
  final def getItemIdCol: String = $(itemIdCol)
  
  setDefault(itemIdCol, "item")
  
  /** @group setParam */
  final def setItemIdCol(name: String): this.type = set(itemIdCol, name)
}
