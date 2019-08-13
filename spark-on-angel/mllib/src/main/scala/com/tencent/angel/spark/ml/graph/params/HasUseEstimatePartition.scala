package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasUseEstimatePartition extends Params {

  final val useEstimatePartition = new BooleanParam(this, "useEstimatePartition", "useEstimatePartition")

  final def getUseEstimatePartition: Boolean = $(useEstimatePartition)

  setDefault(useEstimatePartition, false)

  final def setUseEstimatePartition(flag: Boolean): this.type = set(useEstimatePartition, flag)

}
