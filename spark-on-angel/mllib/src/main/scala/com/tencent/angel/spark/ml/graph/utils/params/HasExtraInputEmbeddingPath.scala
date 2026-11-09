package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasExtraInputEmbeddingPath extends Params {
  final val extraInputEmbeddingPath = new Param[String](this, "extraInputEmbeddingPath",
    "extraInputEmbeddingPath")

  final def getExtraInputEmbeddingPath: String = $(extraInputEmbeddingPath)

  setDefault(extraInputEmbeddingPath, null)

  final def setExtraInputEmbeddingPath(in: String): this.type = set(extraInputEmbeddingPath, in)
}
