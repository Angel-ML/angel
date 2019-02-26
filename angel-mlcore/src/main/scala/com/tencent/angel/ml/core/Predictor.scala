package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.math2.utils.LabeledData

trait Predictor {
  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]

  def predict(storage: LabeledData): PredictResult
}
