package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasCompressIndex extends Params {

  final val compressIndex = new IntParam(this, "compressIndex", "index of compress tag")

  final def getCompressIndex(): Int = $(compressIndex)

  setDefault(compressIndex, 2)

  final def setCompressIndex(index: Int): this.type = set(compressIndex, index)

}