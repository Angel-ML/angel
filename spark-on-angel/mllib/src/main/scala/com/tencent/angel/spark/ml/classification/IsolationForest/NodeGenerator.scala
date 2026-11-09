package com.tencent.angel.spark.ml.classification.IsolationForest

import scala.collection.mutable.ArrayBuffer

/**
 * Created by isakjiang on 2020/12/30.
  * NodeGenerator is for split the inner node and create two sub-nodes
  *
  */
class NodeGenerator {

  val PATH_INIT = "1"
  val PREFIX_PATH = " "

  // each sample contains a ArrayBuffer to show its path in the tree
  private var allSamples: Array[(Array[Double], String)] = Array()
  // features which is already used
  private var numFeature: Int = 0
  var rootNode = new NodeData(1, 0, 0.0, false, 2, 3)

  // init for root,default path is 1
  def this(localSamples: Array[Array[Double]]) = {
    this
    numFeature = localSamples(0).length
    allSamples = localSamples.map((_, PATH_INIT))
    val rootSplitInfor = randomFeaureSplit(allSamples)
    rootNode = new NodeData(1, rootSplitInfor._1, rootSplitInfor._2, false, 2, 3)
  }

  // split samples which included in current node into two subsamples and label the path
  // then create left and right node
  def createSubNodes(parentNode: NodeData): (NodeData, NodeData) = {
    // filter the samples
    val nodeId = parentNode.nodeId
    val leftChildNodeId = parentNode.leftChildNodeId
    val rightChildNodeId = parentNode.rightChildNodeId

    val samplesNotCurrentNode = allSamples.filterNot(x => x._2 contains(nodeId.toString))
    val samples2CurrentNode = allSamples.filter(x => x._2 contains(nodeId.toString))
    // split samples into two subsamples,left and right and label the path
    val leftSamples = samples2CurrentNode
      .filter(x => x._1(parentNode.featureId) < parentNode.split)
      .map{x =>
        val newPath = PREFIX_PATH + leftChildNodeId
        (x._1, x._2 + newPath)
      }

    val rightSamples = samples2CurrentNode
      .filter(x => x._1(parentNode.featureId) >= parentNode.split)
      .map{x =>
        val newPath = PREFIX_PATH + rightChildNodeId
        (x._1, x._2 + newPath)
      }

    // update all samples for sub samples has added new path
    val tempSamples = new ArrayBuffer[(Array[Double], String)]
    // other path which not contain current node
    tempSamples ++= samplesNotCurrentNode
    tempSamples ++= leftSamples
    tempSamples ++= rightSamples
    allSamples = tempSamples.toArray

    val leftChildNode = createNode(leftChildNodeId, leftSamples)

    val rightChildNode = createNode(rightChildNodeId, rightSamples)

    (leftChildNode, rightChildNode)
  }

  // random select feature and split value for two samples，
  // note:the split value is selected between the min and max value
  // return split feature Id and split attribute Id
  def randomFeaureSplit(samples: Array[(Array[Double], String)]): (Int, Double) = {

    // special for selected features
    var validFeatures = (0 until numFeature).toSet
    var splitFeature = scala.util.Random.nextInt(numFeature)
    var selectedFeatures = samples.map(x => x._1(splitFeature))
    // reselect
    while(selectedFeatures.distinct.size == 1){
      validFeatures -= splitFeature
      splitFeature = randomSelected(validFeatures.toArray)
      selectedFeatures = samples.map(x => x._1(splitFeature))
    }

    // find the min and max value in case of being selected
    val minFeature = selectedFeatures.min
    val maxFeature = selectedFeatures.max

    val splitAttr = if(selectedFeatures.distinct.size == 2){
      (minFeature + maxFeature) / 2
    }else{
      // value between min and max value
      val middleFeatures = selectedFeatures.filter(x => (x != minFeature) && (x != maxFeature))
      val tempAttrId = scala.util.Random.nextInt(middleFeatures.length)

      middleFeatures(tempAttrId)
    }

    (splitFeature, splitAttr)
  }

  // check whether it's a leaf node by the way of the children,distinct it
  def isLeafNode(samples: Array[(Array[Double], String)]): Boolean = {

    var isLeaf = true
    val numFeature = samples(0)._1.length
    val samples2Feature = samples.map(_._1)
    var i = 0

    while(i < numFeature && isLeaf){
      val distinctSize = samples2Feature.map(example => example(i)).distinct.size

      if(distinctSize > 1) isLeaf = false
      i += 1
    }
    isLeaf
  }

  // create child node,in case of samples2Parent is null
  def createNode(parentId: Int,
                 samples2Parent: Array[(Array[Double], String)]): NodeData = {

    val leftChildId = 2 * parentId
    val rightChildId = 2 * parentId + 1
    val isLeaf = isLeafNode(samples2Parent)
    val splitInfor = if(isLeaf) (-1, 0.0) else randomFeaureSplit(samples2Parent)


   new NodeData(parentId,
     splitInfor._1,
     splitInfor._2,
     isLeaf,
     leftChildId,
     rightChildId)
  }

  // create a random data from a array
  def randomSelected(datas: Array[Int]): Int = {
    val numData = datas.size
    val randomId = scala.util.Random.nextInt(numData)
    datas(randomId)
  }

}
