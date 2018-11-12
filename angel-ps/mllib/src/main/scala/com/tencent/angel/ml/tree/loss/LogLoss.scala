package com.tencent.angel.ml.tree.loss

import org.apache.spark.mllib.util.MLUtils

/**
  * Class for log loss calculation (for classification).
  * This uses twice the binomial negative log likelihood, called "deviance" in Friedman (1999).
  *
  * The log loss is defined as:
  *   2 log(1 + exp(-2 y F(x)))
  * where y is a label in {-1, 1} and F(x) is the model prediction for features x.
  */
object LogLoss extends ClassificationLoss {

  /**
    * Method to calculate the loss gradients for the gradient boosting calculation for binary
    * classification
    * The gradient with respect to F(x) is: - 4 y / (1 + exp(2 y F(x)))
    * @param prediction Predicted label.
    * @param label True label.
    * @return Loss gradient
    */
  override def gradient(prediction: Double, label: Double): Double = {
    - 4.0 * label / (1.0 + math.exp(2.0 * label * prediction))
  }

  override private[spark] def computeError(prediction: Double, label: Double): Double = {
    val margin = 2.0 * label * prediction
    // The following is equivalent to 2.0 * log(1 + exp(-margin)) but more numerically stable.
    if (-margin > 0) {
      2 * (-margin + math.log1p(math.exp(margin)))
    } else {
      2 * math.log1p(math.exp(-margin))
    }
    2.0 * MLUtils.log1pExp(-margin)
  }

  /**
    * Returns the estimated probability of a label of 1.0.
    */
  override private[spark] def computeProbability(margin: Double): Double = {
    1.0 / (1.0 + math.exp(-2.0 * margin))
  }
}

