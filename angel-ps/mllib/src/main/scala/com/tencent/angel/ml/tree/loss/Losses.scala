package com.tencent.angel.ml.tree.loss

object Losses {

  def fromString(name: String): Loss = name match {
    case "leastSquaresError" => SquaredError
    case "leastAbsoluteError" => AbsoluteError
    case "logLoss" => LogLoss
    case _ => throw new IllegalArgumentException(s"Did not recognize Loss name: $name")
  }

}
