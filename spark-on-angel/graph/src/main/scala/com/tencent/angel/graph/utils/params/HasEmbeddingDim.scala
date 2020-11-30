package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasEmbeddingDim extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val embedding = new IntParam(this, "embedding", "embedding")

  /** @group getParam */
  final def getEmbedding: Int = $(embedding)

  setDefault(embedding, 128)

  /** @group setParam */
  final def setEmbedding(dim: Int): this.type = set(embedding, dim)
}
