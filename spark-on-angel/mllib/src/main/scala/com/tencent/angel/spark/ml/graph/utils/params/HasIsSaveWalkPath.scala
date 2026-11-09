package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}


trait HasIsSaveWalkPath extends Params{
  /**
    * Param for isCompressed.
    *
    * @group param
    */
  final val isSaveWalkPath = new BooleanParam(this, "isSaveWalkPath", "need to save walk path or not")

  final def getIsSaveWalkPath : Boolean = $(isSaveWalkPath)

  final def setIsSaveWalkPath (bool: Boolean): this.type = set(isSaveWalkPath, bool)
}