package com.tencent.angel.ml.tree.oldmodel

import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.tree
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.EnsembleCombiningStrategy._
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext

/**
  * Represents a random forest model.
  *
  * @param algo algorithm for the ensemble model, either Classification or Regression
  * @param trees tree ensembles
  */
class RandomForestModel (
                          override val algo: Algo,
                          override val trees: Array[DecisionTreeModel],
                          conf: Configuration, _ctx: TaskContext = null)
  extends TreeEnsembleModel(algo, trees, Array.fill(trees.length)(1.0),
    combiningStrategy = if (algo == Classification) Vote else Average, conf, _ctx) {

  require(trees.forall(_.algo == algo))
}

/**
  * Represents a tree ensemble model.
  *
  * @param algo algorithm for the ensemble model, either Classification or Regression
  * @param trees tree ensembles
  * @param treeWeights tree ensemble weights
  * @param combiningStrategy strategy for combining the predictions, not used for regression.
  */
sealed class TreeEnsembleModel(
                                protected val algo: Algo,
                                protected val trees: Array[DecisionTreeModel],
                                protected val treeWeights: Array[Double],
                                protected val combiningStrategy: EnsembleCombiningStrategy,
                                conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  require(numTrees > 0, "TreeEnsembleModel cannot be created without trees.")

  private val sumWeights = math.max(treeWeights.sum, 1e-15)

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

  /**
    * Get number of trees in ensemble.
    */
  def numTrees: Int = trees.length

  /**
    * Get total number of nodes, summed over all trees in the ensemble.
    */
  def totalNumNodes: Int = trees.map(_.numNodes).sum
}

object TreeEnsembleModel {

  object SaveUtils {

    import com.tencent.angel.ml.tree.oldmodel.DecisionTreeModel.DataStruct.{NodeData, constructTrees}

    case class Metadata(
                         algo: String,
                         treeAlgo: String,
                         combiningStrategy: String,
                         treeWeights: Array[Double])

    /**
      * Model data for model import/export.
      * We have to duplicate NodeData here since Spark SQL does not yet support extracting subfields
      * of nested fields; once that is possible, we can use something like:
      *  case class EnsembleNodeData(treeId: Int, node: NodeData),
      *  where NodeData is from DecisionTreeModel.
      */
    case class EnsembleNodeData(treeId: Int, node: NodeData)

    def save(path: String, model: TreeEnsembleModel, className: String): Unit = {

      // Create JSON metadata.
      val ensembleMetadata = Metadata(model.algo.toString, model.trees(0).algo.toString,
        model.combiningStrategy.toString, model.treeWeights)
    }

    /**
      * Load trees for an ensemble, and return them in order.
      * @param path path to load the model from
      * @param treeAlgo Algorithm for individual trees (which may differ from the ensemble's
      *                 algorithm).
      */
    def loadTrees(
                   path: String,
                   treeAlgo: String): Array[tree.DecisionTreeModel] = ???
  }

}
