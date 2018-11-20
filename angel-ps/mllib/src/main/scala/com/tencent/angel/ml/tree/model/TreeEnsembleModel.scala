package com.tencent.angel.ml.tree.model

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.tree.conf.Algo.{Algo, Classification, Regression}
import com.tencent.angel.ml.tree.conf.EnsembleStrategy.{Average, EnsembleStrategy, Sum, Vote}
import com.tencent.angel.ml.tree.data._
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.reflect.ClassTag

private[tree] object TreeEnsembleModel {

  case class Metadata(
                       algo: String,
                       treeAlgo: String,
                       combiningStrategy: String,
                       treeWeights: Array[Double])

  /**
    * Given a tree ensemble model, compute the importance of each feature.
    * This generalizes the idea of "Gini" importance to other losses,
    * following the explanation of Gini importance from "Random Forests" documentation
    * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
    *
    * For collections of trees, including boosting and bagging, Hastie et al.
    * propose to use the average of single tree importances across all trees in the ensemble.
    *
    * This feature importance is calculated as follows:
    *  - Average over trees:
    *     - importance(feature j) = sum (over nodes which split on feature j) of the gain,
    *       where gain is scaled by the number of instances passing through node
    *     - Normalize importances for tree to sum to 1.
    *  - Normalize feature importance vector to sum to 1.
    *
    *  References:
    *  - Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.
    *
    * @param trees  Unweighted collection of trees
    * @param numFeatures  Number of features in model (even if not all are explicitly used by
    *                     the model).
    *                     If -1, then numFeatures is set based on the max feature index in all trees.
    * @return  Feature importance values, of length numFeatures.
    */
  def featureImportances[M <: DecisionTreeModel](trees: Array[M], numFeatures: Int): IntFloatVector = {
    val totalImportances = mutable.Map[Int, Float]()
    trees.foreach { tree =>
      // Aggregate feature importance vector for this tree
      val importances = mutable.Map[Int, Float]()
      computeFeatureImportance(tree.rootNode, importances)
      // Normalize importance vector for this tree, and add it to total.
      // TODO: In the future, also support normalizing by tree.rootNode.impurityStats.count?
      val treeNorm = importances.values.sum
      if (treeNorm != 0) {
        importances.foreach { case (idx, impt) =>
          val normImpt = impt / treeNorm
          if (!totalImportances.contains(idx)) totalImportances += (idx -> normImpt)
          else totalImportances.update(idx, totalImportances(idx) + normImpt)
        }
      }
    }
    // Normalize importances
    normalizeMapValues(totalImportances)
    // Construct vector
    val d = if (numFeatures != -1) {
      numFeatures
    } else {
      // Find max feature index used in trees
      val maxFeatureIndex = trees.map(_.maxSplitFeatureIndex()).max
      maxFeatureIndex + 1
    }
    if (d == 0) {
      assert(totalImportances.isEmpty, s"Unknown error in computing feature" +
        s" importance: No splits found, but some non-zero importances.")
    }
    val (indices, values) = totalImportances.iterator.toSeq.sortBy(_._1).unzip
    VFactory.sparseFloatVector(d, indices.toArray, values.toArray)
  }

  /**
    * Given a Decision Tree model, compute the importance of each feature.
    * This generalizes the idea of "Gini" importance to other losses,
    * following the explanation of Gini importance from "Random Forests" documentation
    * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
    *
    * This feature importance is calculated as follows:
    *  - importance(feature j) = sum (over nodes which split on feature j) of the gain,
    *    where gain is scaled by the number of instances passing through node
    *  - Normalize importances for tree to sum to 1.
    *
    * @param tree  Decision tree to compute importances for.
    * @param numFeatures  Number of features in model (even if not all are explicitly used by
    *                     the model).
    *                     If -1, then numFeatures is set based on the max feature index in all trees.
    * @return  Feature importance values, of length numFeatures.
    */
  def featureImportances[M <: DecisionTreeModel : ClassTag](tree: M, numFeatures: Int): IntFloatVector = {
    featureImportances(Array(tree), numFeatures)
  }

  /**
    * Recursive method for computing feature importances for one tree.
    * This walks down the tree, adding to the importance of 1 feature at each node.
    *
    * @param node  Current node in recursion
    * @param importances  Aggregate feature importances, modified by this method
    */
  def computeFeatureImportance(
                                node: Node,
                                importances: mutable.Map[Int, Float]): Unit = {
    node match {
      case n: InternalNode =>
        val feature = n.split.featureIndex
        val scaledGain = n.gain * n.impurityStats.count
        if (!importances.contains(feature)) importances += (feature -> scaledGain)
        else importances.update(feature, importances(feature) + scaledGain)
        computeFeatureImportance(n.leftChild, importances)
        computeFeatureImportance(n.rightChild, importances)
      case n: LeafNode =>
      // do nothing
    }
  }

  /**
    * Normalize the values of this map to sum to 1, in place.
    * If all values are 0, this method does nothing.
    *
    * @param map  Map with non-negative values.
    */
  def normalizeMapValues(map: mutable.Map[Int, Float]): Unit = {
    val total = map.values.sum
    if (total != 0) {
      val keys = map.iterator.map(_._1).toArray
      keys.foreach { key =>
        if (!map.contains(key)) map += (key -> 0.0f) else map.update(key, map(key) / total)
      }
    }
  }
}


/**
  * Represents a tree ensemble model.
  *
  * @param algo algorithm for the ensemble model, either Classification or Regression
  * @param trees tree ensembles
  * @param treeWeights tree ensemble weights
  * @param combiningStrategy strategy for combining the predictions, not used for regression.
  */
class TreeEnsembleModel(
                                protected val algo: Algo,
                                protected val trees: Array[DecisionTreeModel],
                                protected val treeWeights: Array[Double],
                                protected val combiningStrategy: EnsembleStrategy,
                                conf: Configuration,
                                _ctx: TaskContext)
  extends MLModel(conf, _ctx) {

  var treeWeightsVec: IntDoubleVector = VFactory.denseDoubleVector(treeWeights)
  private val sumWeights = math.max(treeWeights.sum, 1e-15)

  require(numTrees > 0, "TreeEnsembleModel cannot be created without trees.")

  super.setSavePath(conf)
  super.setLoadPath(conf)

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val ret = new MemoryDataBlock[PredictResult](-1)
    (0 until dataSet.size).foreach { idx =>
      val instance = dataSet.read
      val x: IntFloatVector = instance.getX.asInstanceOf[IntFloatVector]
      val y = instance.getY
      val pred = predict(x)
      ret.put(DecisionTreePredictResult(idx, y, pred))
    }
    ret
  }

  /**
    * Predicts for a single data point using the weighted sum of ensemble predictions.
    *
    * @param features array representing a single data point
    * @return predicted category from the trained model
    */
  private def predictBySumming(features: IntFloatVector): Double = {
    trees.zip(treeWeights).map{ case (tree, weight) =>
      tree.predict(features) * weight
    }.sum
  }

  /**
    * Classifies a single data point based on (weighted) majority votes.
    */
  private def predictByVoting(features: IntFloatVector): Double = {
    val votes = mutable.Map.empty[Int, Double]
    trees.view.zip(treeWeights).foreach { case (tree, weight) =>
      val prediction = tree.predict(features).toInt
      votes(prediction) = votes.getOrElse(prediction, 0.0) + weight
    }
    votes.maxBy(_._2)._1
  }

  /**
    * Predict values for a single data point using the model trained.
    *
    * @param features array representing a single data point
    * @return predicted category from the trained model
    */
  def predict(features: IntFloatVector): Double = {
    (algo, combiningStrategy) match {
      case (Regression, Sum) =>
        predictBySumming(features)
      case (Regression, Average) =>
        predictBySumming(features) / sumWeights
      case (Classification, Sum) => // binary classification
        val prediction = predictBySumming(features)
        // TODO: predicted labels are +1 or -1 for GBT. Need a better way to store this info.
        if (prediction > 0.0) 1.0 else 0.0
      case (Classification, Vote) =>
        predictByVoting(features)
      case _ =>
        throw new IllegalArgumentException(
          "TreeEnsembleModel given unsupported (algo, combiningStrategy) combination: " +
            s"($algo, $combiningStrategy).")
    }
  }

  /**
    * Predict values for the given data set.
    *
    * @param features RDD representing data points to be predicted
    * @return RDD[Double] where each entry contains the corresponding prediction
    */
  def predict(features: List[IntFloatVector]): List[Double] = features.map(x => predict(x))

  def predict(features: Array[IntFloatVector]): Array[Double] = {
    predict(features.toList).toArray
  }

  /**
    * Get number of trees in ensemble.
    */
  def numTrees: Int = trees.length

  /**
    * Get total number of nodes, summed over all trees in the ensemble.
    */
  def totalNumNodes: Int = trees.map(_.numNodes).sum

  /**
    * Print a summary of the model.
    */
  override def toString: String = {
    algo match {
      case Classification =>
        s"TreeEnsembleModel classifier with $numTrees trees\n"
      case Regression =>
        s"TreeEnsembleModel regressor with $numTrees trees\n"
      case _ => throw new IllegalArgumentException(
        s"TreeEnsembleModel given unknown algo parameter: $algo.")
    }
  }

  /**
    * Print the full model to a string.
    */
  def toDebugString: String = {
    val header = toString + "\n"
    header + trees.zipWithIndex.map { case (tree, treeIndex) =>
      s"  Tree $treeIndex:\n" + tree.topNode.subtreeToString(4)
    }.fold("")(_ + _)
  }
}
