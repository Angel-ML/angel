package com.tencent.angel.spark.ml.classification.IsolationForest

import com.tencent.angel.spark.ml.util._
import org.apache.spark.ml.linalg.{Vector => mlVector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions, _}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer


/**
 * Created by isakjiang on 2020/12/30.
 * Isolation forest is for continues features
 */

object IForestLocal {

  // which percenty can express outliers
  val SCORE_AT_PERCENTILE = "scoreAtPercentile"
  // how many examples each tree should sample
  val NUM_SAMPLE_TREE = "numSampleTree"
  // how many tree to construct forest
  val NUM_TREES = "numTrees"
  // the limit of height for each tree
  val LIMIT_HEIGHT = "limitHeight"
  // Euler's constant
  val EULER_CONSTANT: Double = 0.5772156649
  // interval path
  val INTERVAL_PATH = " "
  // interval to concat node attribute
  val NODE_CONCAT = "_"
  val MODEL_OUT = "modelOut"
  val MODEL_IN = "modelIn"

  def main(args: Array[String]): Unit = {


    val mode = "local"
    val input = "data/mlTestData/anomaly_detection_X_train.csv"
    val partitionNum = 1
    val sampleRate = 1
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outIForest"
    val modelIn = null
    val modelOut = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outModelIForest"
    val colStr = null
    val numSampleTree = 64
    val numTrees = 2
    val sep = ","
    var limitHeight = 64
    // update limit height
    limitHeight = Math.min(limitHeight, numSampleTree)

    val conf = new SparkConf().setMaster(mode).setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    run(sc,
      input,
      output,
      modelIn,
      modelOut,
      partitionNum,
      sampleRate,
      colStr,
      numSampleTree,
      numTrees,
      limitHeight,
      sep)

    sc.stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          modelIn: String,
          modelOut: String,
          partitionNum: Int,
          sampleRate: Double,
          colStr: String,
          numSampleTree: Int,
          numTrees: Int,
          limitHeight: Int,
          sep: String) = {

    // load all data which contains features column
    val dataDF = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, colStr, false, sep)
    val inputCount = dataDF.count

    // train each isolation tree in local
    val forestModel = if (modelIn == null) {

      val sampleTotalNum = numSampleTree * numTrees
      if (sampleTotalNum > inputCount || sampleTotalNum == inputCount)
        throw new Exception(s"the number of input is: $inputCount, " +
          s"but you want to sample $sampleTotalNum samples, " +
          s"the product of number of tree and samples should be smaller than the number of input")

      val allSamples = dataDF
        .select(DFStruct.FEATURE)
        .rdd
        .takeSample(false, sampleTotalNum)

      val allSamplesRdd = sc.makeRDD(allSamples, partitionNum)

      val forestRdd = allSamplesRdd
        .zipWithIndex
        .map { case (Row(feature: mlVector), index) =>
          val newId = index / numSampleTree

          // transform the feature to keep 12 bits after decimal point
          val featureTransPoint = feature.toArray.map(_.formatted("%.12f").toDouble)
          (newId, featureTransPoint)
        }.groupByKey

      val forestCollect = forestRdd.map { case (treeId, sampleData) =>
        println("the number of samples for each tree" + sampleData.size)
        val generator = new NodeGenerator(sampleData.toArray)
        val root = generator.rootNode
        // total nodes for each tree
        val nodesInTree = new ArrayBuffer[NodeData]
        nodesInTree += root

        //flag which index the node in the tree
        var flag = 0

        // decide whether to split a node to add the height of tree
        while (flag < nodesInTree.length) {
          val tempNode = nodesInTree(flag)
          val currentHeight = node2Height(tempNode.nodeId)

          // the termination condition for create a itree
          if (!tempNode.isLeafNode && currentHeight < limitHeight) {
            val (leftChildNode, rightChildNode) = generator.createSubNodes(tempNode)
            nodesInTree += leftChildNode
            nodesInTree += rightChildNode
          }
          flag += 1
        }

        // update leaf nodes which is on the limitHeight
        nodesInTree.map { node =>
          if (node2Height(node.nodeId) == limitHeight) node.isLeafNode = true
          node
        }.toArray
      }.collect()

      // save the forest model and sample number, total number
      val modelStr = forestCollect.map { tree2Node =>
        tree2Node.map(node => node.node2Str(NODE_CONCAT))
      }

      val modelRdd = sc.makeRDD(modelStr, 1)
      DataSaver.save(modelRdd, modelOut, sep)

      forestCollect

    } else {
      // parse the model path
      parseModel(sc, modelIn)
    }

    println("the number of input in each tree is:" + inputCount)
    // predict
    // forest path:the path length from root to external node + the external node Id
    val feature2Path = udf { (items: mlVector) =>
      val oneFeature = items.toArray.map(_.toString.toDouble)
      getForestPath(forestModel, oneFeature)
    }

    // the path of each sample in the tree
    val input2PathLeaf = dataDF
      .withColumn(ParamKeys.OUTPUT, feature2Path(functions.col(DFStruct.FEATURE)))
      .drop(DFStruct.FEATURE)
      .cache()

    // get leaf node score in each tree
    val sample2Leaf = input2PathLeaf
      .select(ParamKeys.OUTPUT).rdd
      .flatMap { case Row(row: String) =>
        val pathAndLeaf = row.split(INTERVAL_PATH)
        val treeCount = pathAndLeaf.length / 2
        // the path length of each example for each tree
        (0 until treeCount).map(i => (i, pathAndLeaf(2 * i + 1)))
      }

    // this is the c(T.size)
    val leafScoreForest = sample2Leaf
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect
      .map(it => (it._1, pathLength(it._2.toDouble)))
      .toMap

    // score for each example,this is E(h(x))
    val example2Score = udf { predictPaths: String =>
      // the predict information of each example
      val pathAndLeaf = predictPaths.split(INTERVAL_PATH)
      val treeCount = pathAndLeaf.length / 2
      val pathSum = (0 until treeCount).map { i =>
        val e = pathAndLeaf(2 * i).toDouble
        val leafScore = leafScoreForest.getOrElse((i, pathAndLeaf(2 * i + 1)), 0.0)
        e + leafScore
      }.sum

      val avePath = pathSum / treeCount
      // score in the end
      Math.pow(2, -1 * avePath / pathLength(inputCount.toDouble))
    }

    val resultRdd = input2PathLeaf.withColumn(ParamKeys.OUTPUT, example2Score(functions.col(ParamKeys.OUTPUT)))

    // save the result
    DataSaver.save(resultRdd, output, sep)

    input2PathLeaf.unpersist

  }

  // compute the height of the current node in the tree
  def node2Height(nodeId: Int): Int = Math.ceil(Math.log(nodeId + 1) / Math.log(2)).toInt

  // return path length in each tree and the leaf node id,
  // and concat the result of all the trees
  def getForestPath(forestCollect: Array[Array[NodeData]],
                    oneFeature: Array[Double]): String = {
    forestCollect.map { tree =>
      val pathInTree = getPath(tree, oneFeature)
      pathInTree.length + INTERVAL_PATH + pathInTree.last
    }.mkString(INTERVAL_PATH)
  }

  def getPath(tree: Array[NodeData], oneFeature: Array[Double]): Array[Int] = {

    val path: ArrayBuffer[Int] = new ArrayBuffer[Int]

    // from root
    var nextNodeId: Int = 1
    var currentNode: NodeData = tree(0)
    var tempFeature: Int = 0
    var tempAttr: Double = 0.0

    // if it's not a leaf node
    while (!currentNode.isLeafNode) {
      tempFeature = currentNode.featureId
      tempAttr = currentNode.split

      if (oneFeature(tempFeature) < tempAttr) {
        nextNodeId = currentNode.leftChildNodeId
        path += nextNodeId
      } else {
        nextNodeId = currentNode.rightChildNodeId
        path += nextNodeId
      }
      currentNode = tree.filter(x => x.nodeId == nextNodeId).head
    }
    path.toArray
  }

  // c(x)
  def pathLength(n: Double): Double = {
    if (n > 2.0)
      2 * (Math.log(n - 1) + EULER_CONSTANT) - (2 * (n - 1) / n)
    else if (n == 2.0)
      1.0
    else
      0.0
  }

  // parse the forest model
  def parseModel(sc: SparkContext, path: String): Array[Array[NodeData]] = {

    val forestModel = sc.textFile(path, 1)
      .collect()
      .map { line =>
        val tree = line.trim.split(INTERVAL_PATH)
        val tree2Nodes = tree.map { nodeStr =>
          val nodeAttr = nodeStr.split(NODE_CONCAT)
          new NodeData(nodeAttr(0).toInt,
            nodeAttr(1).toInt,
            nodeAttr(2).toDouble,
            nodeAttr(3).toBoolean,
            nodeAttr(4).toInt,
            nodeAttr(5).toInt)
        }
        tree2Nodes
      }

    forestModel
  }

}





