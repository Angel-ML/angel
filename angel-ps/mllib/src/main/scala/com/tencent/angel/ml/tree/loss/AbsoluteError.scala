package com.tencent.angel.ml.tree.loss

/**
  * Class for absolute error loss calculation (for regression).
  *
  * The absolute (L1) error is defined as:
  *  |y - F(x)|
  * where y is the label and F(x) is the model prediction for features x.
  */
object AbsoluteError extends Loss {

  /**
    * Method to calculate the gradients for the gradient boosting calculation for least
    * absolute error calculation.
    * The gradient with respect to F(x) is: sign(F(x) - y)
    * @param prediction Predicted label.
    * @param label True label.
    * @return Loss gradient
    */
  override def gradient(prediction: Double, label: Double): Double = {
    if (label - prediction < 0) 1.0 else -1.0
  }

  override def computeError(prediction: Double, label: Double): Double = {
    val err = label - prediction
    math.abs(err)
  }
}

