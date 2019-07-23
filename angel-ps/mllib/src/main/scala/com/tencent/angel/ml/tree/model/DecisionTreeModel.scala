package com.tencent.angel.ml.tree.model

import scala.beans.BeanProperty
import java.text.DecimalFormat

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.tree.conf.Algo.{Algo, Classification, Regression}
import com.tencent.angel.ml.tree.data._
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

object DecisionTreeModel {

  val FEAT_CATEGORY_MAT = "gbdt.feature.category"
  val SPLIT_FEAT_MAT: String = "gbdt.split.feature"
  val SPLIT_VALUE_MAT: String = "gbdt.split.value"
  val SPLIT_GAIN_MAT: String = "gbdt.split.gain"
  val NODE_PRED_MAT: String = "gbdt.node.predict"

  def apply(topNode: Node, algo: Algo, conf: Configuration): DecisionTreeModel = {
    new DecisionTreeModel(topNode, algo, conf, null)
  }

  def apply(topNode: Node, algo: Algo, conf: Configuration, ctx: TaskContext): DecisionTreeModel = {
    new DecisionTreeModel(topNode, algo, conf, ctx)
  }
}

/**
  * Decision tree model for classification or regression.
  * This model stores the decision tree structure and parameters.
  *
  * @param topNode root node
  * @param algo algorithm type -- classification or regression
  */
class DecisionTreeModel (
                          @BeanProperty var topNode: Node,
                          @BeanProperty var algo: Algo,
                          conf: Configuration,
                          _ctx: TaskContext) extends MLModel(conf, _ctx) {

  super.setSavePath(conf)
  super.setLoadPath(conf)

  /** Root of the decision tree */
  def rootNode: Node = topNode

  /** Number of nodes in tree, including leaf nodes. */
  def numNodes: Int = {
    1 + topNode.numDescendants
  }

  /**
    * Depth of tree.
    * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
    */
  def depth: Int = {
    topNode.subtreeDepth
  }

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val ret = new MemoryDataBlock[PredictResult](-1)
    (0 until dataSet.size).foreach { idx =>
      val instance = dataSet.read
      val x: IntFloatVector = instance.getX.asInstanceOf[IntFloatVector]
      val y = instance.getY
      val pred = predict(x)
      ret.put(DecisionTreePredictResult(idx.toString, y, pred))
    }
    ret
  }

  /**
    * Predict values for a single data point using the model trained.
    *
    * @param features array representing a single data point
    * @return Double prediction from the trained model
    */
  def predict(features: IntFloatVector): Float = {
    topNode.predictImpl(features).prediction
  }

  /**
    * Predict values for the given data set using the model trained.
    *
    * @param features RDD representing data points to be predicted
    * @return RDD of predictions for each of the given data points
    */
  def predict(features: List[IntFloatVector]): List[Float] = {
    features.map(x => predict(x))
  }

  /**
    * Trace down the tree, and return the largest feature index used in any split.
    *
    * @return  Max feature index used in a split, or -1 if there are no splits (single leaf node).
    */
  def maxSplitFeatureIndex(): Int = rootNode.maxSplitFeatureIndex()

  /**
    * Print a summary of the model.
    */
  override def toString: String = algo match {
    case Classification =>
      s"DecisionTreeModel classifier of depth $depth with $numNodes nodes"
    case Regression =>
      s"DecisionTreeModel regressor of depth $depth with $numNodes nodes"
    case _ => throw new IllegalArgumentException(
      s"DecisionTreeModel given unknown algo parameter: $algo.")
  }

  /**
    * Print the full model to a string.
    */
  def toDebugString: String = {
    val header = toString + "\n"
    header + topNode.subtreeToString(2)
  }
}

case class DecisionTreePredictResult(sid: String, pred: Double, label: Double) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText: String = {
    df.format(sid) + separator + format.format(pred) + separator + df.format(label)
  }
}