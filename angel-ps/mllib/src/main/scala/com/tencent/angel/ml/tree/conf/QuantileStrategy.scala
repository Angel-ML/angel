package com.tencent.angel.ml.tree.conf

/**
  * Enum for selecting the quantile calculation strategy
  */
object QuantileStrategy extends Enumeration {
  type QuantileStrategy = Value
  val Sort, MinMax, ApproxHist = Value
}
