package com.tencent.angel.ml.tree.model

import scala.collection.mutable


/**
  * This is used by the node id cache to find the child id that a data point would belong to.
  * @param split Split information.
  * @param nodeIndex The current node index of a data point that this will update.
  */
private[tree] case class NodeIndexUpdater(split: Split, nodeIndex: Int) {

  /**
    * Determine a child node index based on the feature value and the split.
    * @param binnedFeature Binned feature value.
    * @param splits Split information to convert the bin indices to approximate feature values.
    * @return Child node index to update to.
    */
  def updateNodeIndex(binnedFeature: Int, splits: Array[Split]): Int = {
    if (split.shouldGoLeft(binnedFeature, splits)) {
      LearningNode.leftChildIndex(nodeIndex)
    } else {
      LearningNode.rightChildIndex(nodeIndex)
    }
  }
}

/**
  * Each TreePoint belongs to a particular node per tree.
  * Each row in the nodeIdsForInstances RDD is an array over trees of the node index
  * in each tree. Initially, values should all be 1 for root node.
  * The nodeIdsForInstances RDD needs to be updated at each iteration.
  * @param nodeIdsForInstances The initial values in the cache
  *                           (should be an Array of all 1's (meaning the root nodes)).
  */
private[tree] class NodeIdCache(var nodeIdsForInstances: Array[Array[Int]]) {

  private var updateCount = 0

  /**
    * Update the node index values in the cache.
    * This updates the RDD and its lineage.
    * TODO: Passing bin information to executors seems unnecessary and costly.
    * @param data training rows.
    * @param nodeIdUpdaters A map of node index updaters.
    *                       The key is the indices of nodes that we want to update.
    * @param splits  Split information needed to find child node indices.
    */
  def updateNodeIndices(
                         data: Array[BaggedPoint[TreePoint]],
                         nodeIdUpdaters: Array[mutable.Map[Int, NodeIndexUpdater]],
                         splits: Array[Array[Split]]): Unit = {

    nodeIdsForInstances = data.zip(nodeIdsForInstances).map { case (point, ids) =>
      var treeId = 0
      while (treeId < nodeIdUpdaters.length) {
        val nodeIdUpdater = nodeIdUpdaters(treeId).getOrElse(ids(treeId), null)
        if (nodeIdUpdater != null) {
          val featureIndex = nodeIdUpdater.split.featureIndex
          val newNodeIndex = nodeIdUpdater.updateNodeIndex(
            binnedFeature = point.datum.binnedFeatures(featureIndex),
            splits = splits(featureIndex))
          ids(treeId) = newNodeIndex
        }
        treeId += 1
      }
      ids
    }

    updateCount += 1
  }
}

private[tree] object NodeIdCache {
  /**
    * Initialize the node Id cache with initial node Id values.
    * @param data The Array of training rows.
    * @param numTrees The number of trees that we want to create cache for.
    * @param initVal The initial values in the cache.
    * @return A node Id cache containing an Array of initial root node Indices.
    */
  def init(
            data: Array[BaggedPoint[TreePoint]],
            numTrees: Int,
            initVal: Int = 1): NodeIdCache = {
    new NodeIdCache(
      data.map(_ => Array.fill[Int](numTrees)(initVal)))
  }
}

