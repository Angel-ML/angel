package com.tencent.angel.spark.ml.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasVerbose extends Params {

  final val verbose = new BooleanParam(this, "verbose", "verbose")

  final def getVerbose: Boolean = $(verbose)

  setDefault(verbose, true)

  final def setVerbose(flag: Boolean): this.type = set(verbose, flag)

}
