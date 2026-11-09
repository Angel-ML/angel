package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasExtraOutputEmbeddingPath extends Params {
  final val extraOutputEmbeddingPath = new Param[String](this, "extraOutputEmbeddingPath",
    "extraOutputEmbeddingPath")

  final def getExtraOutputEmbeddingPath: String = $(extraOutputEmbeddingPath)

  setDefault(extraOutputEmbeddingPath, null)

  final def setExtraOutputEmbeddingPath(in: String): this.type = set(extraOutputEmbeddingPath, in)
}