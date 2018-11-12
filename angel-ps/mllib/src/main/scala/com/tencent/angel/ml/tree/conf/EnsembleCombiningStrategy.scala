package com.tencent.angel.ml.tree.conf

/**
  * Enum to select ensemble combining strategy for base learners
  */
private object EnsembleCombiningStrategy extends Enumeration {
  type EnsembleCombiningStrategy = Value
  val Average, Sum, Vote = Value
}

