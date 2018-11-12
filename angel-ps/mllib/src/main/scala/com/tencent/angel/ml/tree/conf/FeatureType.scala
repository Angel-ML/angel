package com.tencent.angel.ml.tree.conf

/**
  * Enum to describe whether a feature is "continuous" or "categorical"
  */
object FeatureType extends Enumeration {
  type FeatureType = Value
  val Continuous, Categorical = Value
}
