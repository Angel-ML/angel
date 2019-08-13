package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasUseBalancePartition extends Params {

  final val useBalancePartition = new BooleanParam(this, "useBalancePartition", "useBalancePartition")

  final def getUseBalancePartition: Boolean = $(useBalancePartition)

  setDefault(useBalancePartition, false)

  final def setUseBalancePartition(flag: Boolean): this.type = set(useBalancePartition, flag)

}
