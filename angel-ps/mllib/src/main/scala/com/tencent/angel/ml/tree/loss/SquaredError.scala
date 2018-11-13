package com.tencent.angel.ml.tree.loss

/**
  * Class for squared error loss calculation.
  *
  * The squared (L2) error is defined as:
  *   (y - F(x))**2
  * where y is the label and F(x) is the model prediction for features x.
  */
object SquaredError extends Loss {

  /**
    * Method to calculate the gradients for the gradient boosting calculation for least
    * squares error calculation.
    * The gradient with respect to F(x) is: - 2 (y - F(x))
    * @param prediction Predicted label.
    * @param label True label.
    * @return Loss gradient
    */
  override def gradient(prediction: Double, label: Double): Double = {
    - 2.0 * (label - prediction)
  }

  override def computeError(prediction: Double, label: Double): Double = {
    val err = label - prediction
    err * err
  }
}

