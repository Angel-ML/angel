package com.tencent.angel.ml.tree.model

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.conf.Algo
import com.tencent.angel.ml.tree.data.{DecisionTreeClassifierParams, Node}
import com.tencent.angel.ml.tree.utils.ProbabilisticUtils
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

object DecisionTreeClassificationModel {

  def apply(topNode: Node, numFeatures: Int,
            numClasses: Int, conf: Configuration): DecisionTreeClassificationModel = {
    new DecisionTreeClassificationModel(topNode, numFeatures, numClasses, conf, _ctx = null)
  }

  def apply(topNode: Node, numFeatures: Int,
            numClasses: Int, conf: Configuration, ctx: TaskContext): DecisionTreeClassificationModel = {
    new DecisionTreeClassificationModel(topNode, numFeatures, numClasses, conf, ctx)
  }
}


/**
  * Decision tree model (http://en.wikipedia.org/wiki/Decision_tree_learning) for classification.
  * It supports both binary and multiclass labels, as well as both continuous and categorical
  * features.
  */
private[tree] class DecisionTreeClassificationModel (
                                                      override val rootNode: Node,
                                                      val numFeatures: Int,
                                                      val numClasses: Int,
                                                      conf: Configuration,
                                                      _ctx: TaskContext)
    extends DecisionTreeModel(rootNode, Algo.Classification, conf, _ctx)
      with DecisionTreeClassifierParams with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  def predictRaw(features: IntFloatVector): IntFloatVector = {
    VFactory.denseFloatVector(rootNode.predictImpl(features).impurityStats.stats.clone())
  }

  protected def raw2probabilityInPlace(rawPrediction: IntFloatVector): IntFloatVector = {
    if (rawPrediction.isDense) {
      ProbabilisticUtils.normalizeToProbabilitiesInPlace(rawPrediction)
      rawPrediction
    } else {
        throw new RuntimeException("Unexpected error in DecisionTreeClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def toString: String = {
    s"DecisionTreeClassificationModel of depth $depth with $numNodes nodes"
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
    * correlated predictor variables. Consider using a RandomForestClassifier
    * to determine feature importance instead.
    */
  val featureImportances: IntFloatVector = TreeEnsembleModel.featureImportances(this, numFeatures)
}

