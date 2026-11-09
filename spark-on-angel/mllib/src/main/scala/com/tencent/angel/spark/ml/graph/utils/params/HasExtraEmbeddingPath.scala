package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasExtraEmbeddingPath extends Params {
  final val extraEmbeddingPath = new Param[String](this, "extraEmbeddingPath",
    "extraEmbeddingPath")

  final def getExtraEmbeddingPath: String = $(extraEmbeddingPath)

  setDefault(extraEmbeddingPath, null)

  final def setExtraEmbeddingPath(in: String): this.type = set(extraEmbeddingPath, in)
}
