package com.tencent.angel.ml.tree.impurity

/**
  * Factory for Impurity instances.
  */
private[tree] object Impurities {

  def fromString(name: String): Impurity = name match {
    case "gini" => Gini
    case "entropy" => Entropy
    case "variance" => Variance
    case _ => throw new IllegalArgumentException(s"Did not recognize Impurity name: $name")
  }

}

