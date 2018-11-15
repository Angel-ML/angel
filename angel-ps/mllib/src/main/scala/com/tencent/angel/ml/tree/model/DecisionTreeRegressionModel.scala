package com.tencent.angel.ml.tree.model


import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.conf.{Algo => OldAlgo}
import com.tencent.angel.ml.tree.TreeEnsembleModel
import com.tencent.angel.ml.tree.DecisionTreeModel
import com.tencent.angel.ml.tree.DecisionTreeRegressorParams
import com.tencent.angel.ml.tree.oldmodel.{DecisionTreeModel => OldDecisionTreeModel}


/**
  * <a href="http://en.wikipedia.org/wiki/Decision_tree_learning">
  * Decision tree (Wikipedia)</a> model for regression.
  * It supports both continuous and categorical features.
  * @param rootNode  Root of the decision tree
  */
private[tree] class DecisionTreeRegressionModel (
                                                val rootNode: Node,
                                                val numFeatures: Int)
    extends DecisionTreeModel with DecisionTreeRegressorParams {

  require(rootNode != null,
    "DecisionTreeRegressionModel given null rootNode, but it requires a non-null rootNode.")

  def predict(features: IntFloatVector): Float = {
    rootNode.predictImpl(features).prediction
  }

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
  lazy val featureImportances: IntFloatVector = TreeEnsembleModel.featureImportances(this, numFeatures)

  /** Convert to spark.mllib DecisionTreeModel (losing some information) */
  def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Regression)
  }
}

object DecisionTreeRegressionModel {

  /** Convert a model from the old API */
  private[ml] def fromOld(
                           oldModel: OldDecisionTreeModel,
                           categoricalFeatures: Map[Int, Int],
                           numFeatures: Int = -1): DecisionTreeRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-regression DecisionTreeModel (old API) to" +
        s" DecisionTreeRegressionModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode, categoricalFeatures)
    new DecisionTreeRegressionModel(rootNode, numFeatures)
  }
}

