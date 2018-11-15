package com.tencent.angel.ml.tree.model

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.{DecisionTreeClassifierParams, DecisionTreeModel, TreeEnsembleModel}
import com.tencent.angel.ml.tree.conf.{Algo => OldAlgo}
import com.tencent.angel.ml.tree.oldmodel.{DecisionTreeModel => OldDecisionTreeModel}

/**
  * Decision tree model (http://en.wikipedia.org/wiki/Decision_tree_learning) for classification.
  * It supports both binary and multiclass labels, as well as both continuous and categorical
  * features.
  */
private[tree] class DecisionTreeClassificationModel (
                                                    val rootNode: Node,
                                                    val numFeatures: Int,
                                                    val numClasses: Int)
    extends DecisionTreeModel with DecisionTreeClassifierParams with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  def predict(features: IntFloatVector): Float = {
    rootNode.predictImpl(features).prediction
  }

  def predictRaw(features: IntFloatVector): IntFloatVector = {
    VFactory.denseFloatVector(rootNode.predictImpl(features).impurityStats.stats.clone())
  }

  protected def raw2probabilityInPlace(rawPrediction: IntFloatVector): IntFloatVector = {
    if (rawPrediction.isDense) {
      ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(rawPrediction)
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
  lazy val featureImportances: IntFloatVector = TreeEnsembleModel.featureImportances(this, numFeatures)

  /** Convert to spark.mllib DecisionTreeModel (losing some information) */
  override private[tree] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Classification)
  }

}

object DecisionTreeClassificationModel {

  /** Convert a model from the old API */
  private[ml] def fromOld(
                           oldModel: OldDecisionTreeModel,
                           categoricalFeatures: Map[Int, Int],
                           numFeatures: Int = -1): DecisionTreeClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification DecisionTreeModel (old API) to" +
        s" DecisionTreeClassificationModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode, categoricalFeatures)
    // Can't infer number of features from old model, so default to -1
    new DecisionTreeClassificationModel(rootNode, numFeatures, -1)
  }
}

