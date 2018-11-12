package com.tencent.angel.ml.tree.conf

/**
  * Enum to select the algorithm for the decision tree
  */
object Algo extends Enumeration {

  type Algo = Value

  val Classification, Regression = Value

  private[mllib] def fromString(name: String): Algo = name match {
    case "classification" | "Classification" => Classification
    case "regression" | "Regression" => Regression
    case _ => throw new IllegalArgumentException(s"Did not recognize Algo name: $name")
  }
}
