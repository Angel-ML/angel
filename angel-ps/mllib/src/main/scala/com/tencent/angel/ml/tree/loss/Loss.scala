package com.tencent.angel.ml.tree.loss

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.model.TreeEnsembleModel

/**
  * Trait for adding "pluggable" loss functions for the gradient boosting algorithm.
  */
trait Loss extends Serializable {

  /**
    * Method to calculate the gradients for the gradient boosting calculation.
    * @param prediction Predicted feature
    * @param label true label.
    * @return Loss gradient.
    */
  def gradient(prediction: Double, label: Double): Double

  /**
    * Method to calculate error of the base learner for the gradient boosting calculation.
    *
    * @param model Model of the weak learner.
    * @param data Training dataset: List of LabeledData
    * @return Measure of model error on data
    *
    * @note This method is not used by the gradient boosting algorithm but is useful for debugging
    * purposes.
    */
  def computeError(model: TreeEnsembleModel, data: List[LabeledData]): Double = {
    data.map(point => computeError(model.predict(point.getX.asInstanceOf[IntFloatVector]), point.getY))
      .reduce(_ + _) / data.length
  }

  /**
    * Method to calculate loss when the predictions are already known.
    *
    * @param prediction Predicted label.
    * @param label True label.
    * @return Measure of model error on datapoint.
    *
    * @note This method is used in the method evaluateEachIteration to avoid recomputing the
    * predicted values from previously fit trees.
    */
  def computeError(prediction: Double, label: Double): Double
}

trait ClassificationLoss extends Loss {
  /**
    * Computes the class probability given the margin.
    */
  def computeProbability(margin: Double): Double
}

