package com.tencent.angel.spark.ml.classification.IsolationForest

/**
 * Created by isakjiang on 2020/12/30.
  */
class NodeData(var nodeId: Int,
               var featureId: Int,
               var split: Double,
               var isLeafNode: Boolean,
               var leftChildNodeId: Int,
               var rightChildNodeId: Int) extends Serializable{

  def node2Str(interval: String): String = {
    Array(nodeId.toString,
      featureId.toString,
      split.toString,
      isLeafNode.toString,
      leftChildNodeId.toString,
      rightChildNodeId.toString
    ).mkString(interval)
  }

}
