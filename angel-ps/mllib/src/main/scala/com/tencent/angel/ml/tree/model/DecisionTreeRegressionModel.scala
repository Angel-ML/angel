package com.tencent.angel.ml.tree.model


import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.conf.Algo
import com.tencent.angel.ml.tree.data.{DecisionTreeRegressorParams, Node}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

object DecisionTreeRegressionModel {

  def apply(topNode: Node, numFeatures: Int, conf: Configuration): DecisionTreeRegressionModel = {
    new DecisionTreeRegressionModel(topNode, numFeatures, conf, _ctx = null)
  }

  def apply(topNode: Node, numFeatures: Int, conf: Configuration, ctx: TaskContext): DecisionTreeRegressionModel = {
    new DecisionTreeRegressionModel(topNode, numFeatures, conf, ctx)
  }
}

/**
  * <a href="http://en.wikipedia.org/wiki/Decision_tree_learning">
  * Decision tree (Wikipedia)</a> model for regression.
  * It supports both continuous and categorical features.
  * @param rootNode  Root of the decision tree
  */
private[tree] class DecisionTreeRegressionModel (
                                                override val rootNode: Node,
                                                val numFeatures: Int,
                                                conf: Configuration,
                                                _ctx: TaskContext)
    extends DecisionTreeModel(rootNode, Algo.Regression, conf, _ctx)
      with DecisionTreeRegressorParams {

  require(rootNode != null,
    "DecisionTreeRegressionModel given null rootNode, but it requires a non-null rootNode.")

  /** We need to update this function if we ever add other impurity measures. */
  def predictVariance(features: IntFloatVector): Float = {
    rootNode.predictImpl(features).impurityStats.calculate()
  }

  override def toString: String = {
    s"DecisionTreeRegressionModel of depth $depth with $numNodes nodes"
  }

  /**
    * Estimate of the importance of each feature.
    *
    * This generalizes the idea of "Gini" importance to other losses,
    * following the explanation of Gini importance from "Random Forests" documentation
    * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
    *
    * This feature importance is calculated as follows:
    *   - importance(feature j) = sum (over nodes which split on feature j) of the gain,
    *     where gain is scaled by the number of instances passing through node
    *   - Normalize importances for tree to sum to 1.
    *
    * @note Feature importance for single decision trees can have high variance due to
    * correlated predictor variables. Consider using a RandomForestRegressor
    * to determine feature importance instead.
    */
  val featureImportances: IntFloatVector = TreeEnsembleModel.featureImportances(this, numFeatures)
}

