package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOldModelPath extends Params {
  final val oldModelPath = new Param[String](this, "oldModelPath", "oldModelPath")

  final def getOldModelPath: String = $(oldModelPath)

  setDefault(oldModelPath, null)

  final def setOldModelPath(in: String): this.type = set(oldModelPath, in)
}
