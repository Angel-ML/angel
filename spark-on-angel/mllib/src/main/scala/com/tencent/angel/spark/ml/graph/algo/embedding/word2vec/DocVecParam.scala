package com.tencent.angel.spark.ml.graph.algo.embedding.word2vec

class DocVecParam extends Word2VecParam {
  var poolingStrategy: String = _

  def setPoolingStrategy(poolingStrategy: String): this.type = {
    this.poolingStrategy = poolingStrategy
    this
  }
}
