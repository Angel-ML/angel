package com.tencent.angel.ml.predict

import java.text.DecimalFormat

object PredictResult{

}

abstract class PredictResult {
  def format: DecimalFormat = new DecimalFormat("0.000000")
  def separator: String = ","

  def getText: String
}