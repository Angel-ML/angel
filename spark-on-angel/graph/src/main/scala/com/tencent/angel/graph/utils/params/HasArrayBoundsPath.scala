package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasArrayBoundsPath extends Params {

  final val arrayBoundsPath = new Param[String](this, "arrayBoundsPath", "arrayBounds path for rangePartitioner")

  final def getArrayBoundsPath: String = $(arrayBoundsPath)

  setDefault(arrayBoundsPath, "")

  final def setArrayBoundsPath(path: String): this.type = set(arrayBoundsPath, path)

}