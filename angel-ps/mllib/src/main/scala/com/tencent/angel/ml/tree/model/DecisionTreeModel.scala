package com.tencent.angel.ml.tree.model

import java.text.DecimalFormat

import scala.collection.mutable
import scala.beans.BeanProperty
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.{Algo, FeatureType}
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

object DecisionTreeModel {

  val SKETCH_MAT: String = "gbdt.sketch"
  val FEAT_SAMPLE_MAT: String = "gbdt.feature.sample"
  val FEAT_CATEGORY_MAT = "gbdt.feature.category"
  val SPLIT_FEAT_MAT: String = "gbdt.split.feature"
  val SPLIT_VALUE_MAT: String = "gbdt.split.value"
  val SPLIT_GAIN_MAT: String = "gbdt.split.gain"
  val NODE_PRED_MAT: String = "gbdt.node.predict"

  def apply(conf: Configuration) = {
    new DecisionTreeModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new DecisionTreeModel(conf, ctx)
  }

  private[mllib] object DataStruct {

    case class PredictData(predict: Float, prob: Float) {
      def toPredict: Predict = new Predict(predict, prob)
    }

    object PredictData {
      def apply(p: Predict): PredictData = PredictData(p.predict, p.prob)

      def apply(vec: IntFloatVector): PredictData = PredictData(vec.get(3), vec.get(4))
    }

    case class SplitData(
                          feature: Int,
                          threshold: Float,
                          featureType: Int,
                          categories: Seq[Float]) {
      def toSplit: Split = {
        new Split(feature, threshold, FeatureType(featureType), categories.toList)
      }
    }

    object SplitData {
      def apply(s: Split): SplitData = {
        SplitData(s.feature, s.threshold, s.featureType.id, s.categories)
      }

      def apply(vec: IntFloatVector): SplitData = {
        SplitData(vec.get(9).toInt, vec.get(10), vec.get(11).toInt, vec.get((12 to vec.dim.toInt).toArray))
      }
    }

    /** Model data for model import/export
      * 0: treeId
      * 1: nodeId (-1 means null)
      * 2: pred
      * 3: pred_prob
      * 4: impurity
      * 5: isLeaf (1=true)
      * 6: leafChildId (-1 means null)
      * 7: rightChildId (-1 means null)
      * 8: infoGain
      * 9: splitFeature
      * 10: splitThreshold
      * 11: splitType
      * 12 to end: categories
      * */
    case class NodeData(
                         treeId: Int,
                         nodeId: Int,
                         predict: PredictData,
                         impurity: Float,
                         isLeaf: Boolean,
                         leftNodeId: Option[Int],
                         rightNodeId: Option[Int],
                         infoGain: Option[Float],
                         split: Option[SplitData])

    object NodeData {
      def apply(treeId: Int, n: Node): NodeData = {
        NodeData(treeId, n.id, PredictData(n.predict), n.impurity, n.isLeaf, n.leftNode.map(_.id), n.rightNode.map(_.id),
          n.stats.map(_.gain), n.split.map(SplitData.apply))
      }

      def apply(vec: IntFloatVector): NodeData = {
        val leftNodeId = if (vec.get(6) == -1) None else Some(vec.get(6).toInt)
        val rightNodeId = if (vec.get(7) == -1) None else Some(vec.get(7).toInt)
        val infoGain = if (vec.get(8) < 0) None else Some(vec.get(8))
        val split = if (vec.get(9) == -1) None else Some(SplitData(vec))
        NodeData(vec.get(0).toInt, vec.get(1).toInt, PredictData(vec), vec.get(4),
          vec.get(5) == 1.0, leftNodeId, rightNodeId, infoGain, split)
      }
    }

    def constructTrees(nodes: List[NodeData]): Array[Node] = {
      val trees: List[(Int, Node)] = nodes.groupBy(_.treeId)
        .mapValues(_.toArray)
        .map { case (treeId, data) =>
          (treeId, constructTree(data))
        }.toList.sortBy(_._1)
      val numTrees = trees.length
      val treeIndices = trees.map(_._1).toSeq
      assert(treeIndices == (0 until numTrees),
        s"Tree indices must start from 0 and increment by 1, but we found $treeIndices.")
      trees.map(_._2)
    }

    /**
      * Given a list of nodes from a tree, construct the tree.
      * @param data array of all node data in a tree.
      */
    def constructTree(data: Array[NodeData]): Node = {
      val dataMap: Map[Int, NodeData] = data.map(n => n.nodeId -> n).toMap
      assert(dataMap.contains(1),
        s"DecisionTree missing root node (id = 1).")
      constructNode(1, dataMap, mutable.Map.empty)
    }

    /**
      * Builds a node from the node data map and adds new nodes to the input nodes map.
      */
    private def constructNode(
                               id: Int,
                               dataMap: Map[Int, NodeData],
                               nodes: mutable.Map[Int, Node]): Node = {
      if (nodes.contains(id)) {
        return nodes(id)
      }
      val data = dataMap(id)
      val node =
        if (data.isLeaf) {
          Node(data.nodeId, data.predict.toPredict, data.impurity, data.isLeaf)
        } else {
          val leftNode = constructNode(data.leftNodeId.get, dataMap, nodes)
          val rightNode = constructNode(data.rightNodeId.get, dataMap, nodes)
          val stats = new InformationGainStats(data.infoGain.get, data.impurity, leftNode.impurity,
            rightNode.impurity, leftNode.predict, rightNode.predict)
          new Node(data.nodeId, data.predict.toPredict, data.impurity, data.isLeaf,
            data.split.map(_.toSplit), Some(leftNode), Some(rightNode), Some(stats))
        }
      nodes += node.id -> node
      node
    }
  }

}

/**
  * Decision tree model for classification or regression.
  * This model stores the decision tree structure and parameters.
  * @param topNode root node
  * @param algo algorithm type -- classification or regression
  */
class DecisionTreeModel (@BeanProperty val topNode: Node, val algo: Algo, conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  super.setSavePath(conf)
  super.setLoadPath(conf)

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val ret = new MemoryDataBlock[PredictResult](-1)

    (0 until dataSet.size).foreach { idx =>
      val instance = dataSet.read
      val x: IntFloatVector = instance.getX.asInstanceOf[IntFloatVector]
      val y = instance.getY
      val pred = predict(x)

      ret.put(new DecisionTreePredictResult(idx, y, pred))
    }

    ret
  }

  /**
    * Predict values for a single data point using the model trained.
    *
    * @param features array representing a single data point
    * @return Double prediction from the trained model
    */
  def predict(features: IntFloatVector): Double = {
    topNode.predict(features)
  }

  /**
    * Predict values for the given data set using the model trained.
    *
    * @param features RDD representing data points to be predicted
    * @return RDD of predictions for each of the given data points
    */
  def predict(features: List[IntFloatVector]): List[Double] = {
    features.map(x => predict(x))
  }

  /**
    * Get number of nodes in tree, including leaf nodes.
    */
  def numNodes: Int = {
    1 + topNode.numDescendants
  }

  /**
    * Get depth of tree.
    * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
    */
  def depth: Int = {
    topNode.subtreeDepth
  }

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

case class DecisionTreePredictResult(sid: Long, pred: Double, label: Double) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText: String = {
    df.format(sid) + separator + format.format(pred) + separator + df.format(label)
  }
}
