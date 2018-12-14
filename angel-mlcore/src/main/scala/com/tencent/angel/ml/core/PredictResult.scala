package com.tencent.angel.ml.core

class PredictResult(val sid: String, val pred: Double, val proba: Double, val predLabel: Double,
                    val trueLabel: Double, val attached: Double) {
  var separator = ", "

  def getText: String = {
    val predStr = if (pred == Double.NaN) "" else f"$pred%.2f$separator"
    val probaStr = if (proba == Double.NaN) "" else f"$proba%.2f$separator"
    val trueLabelStr = if (trueLabel == Double.NaN) "" else f"$trueLabel%.2f"
    f"$sid%s$separator$predLabel%.2f$predStr$probaStr$trueLabelStr"
  }
}

object PredictResult {
  def apply(sid: String, pred: Double, proba: Double, predLabel: Double,
            trueLabel: Double, attached: Double): PredictResult = {
    new PredictResult(sid, pred, proba, predLabel, trueLabel, attached)
  }
}
