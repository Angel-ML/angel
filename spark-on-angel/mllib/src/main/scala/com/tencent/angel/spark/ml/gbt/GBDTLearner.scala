/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.ml.gbt

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.yahoo.sketches.quantiles.DoublesSketch
import org.apache.commons.logging.LogFactory
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import com.tencent.angel.ml.GBDT.psf.{GBDTGradHistGetRowFunc, GBDTGradHistGetRowResult, HistAggrParam}
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.spark.ml.common.{Instance, Learner}
import com.tencent.angel.spark.models.MLModel
import com.tencent.angel.spark.models.matrix.{DensePSMatrix, PSMatrix}

class GBDTLearner(@transient val param: GBTreeParam) extends Learner {
  val LOG = LogFactory.getLog(classOf[GBDTLearner])

  // environment parameter
  @transient private val spark = SparkSession.builder().getOrCreate()
  private val paramBC = spark.sparkContext.broadcast(param)
  @transient private val forest: ArrayBuffer[Tree] = new ArrayBuffer[Tree]()
  @transient private[gbt] val gbtModel: GBDTModel = new GBDTModel(param)

  private[gbt] var instanceLayoutMat: DensePSMatrix = _
  private[gbt] val instancePosInfoMat = PSMatrix.dense(param.partitionNum, param.maxNodeNum * 2)

  // TODO: move the following matrix to GBDTModel.
  private[gbt] val splitGainMat = PSMatrix.dense(param.maxTreeNum, param.maxNodeNum)
  private[gbt] val gradHessMat = PSMatrix.dense(param.maxTreeNum, param.maxNodeNum * 2)
  private[gbt] var gradHistMatrix: DensePSMatrix = _

  def train(trainSet: RDD[Instance]): GBDTModel = {
    val (instances, validateSet) = init(trainSet)
    val sketch = createSketch(instances)
    var predictions = instances
      .mapPartitions({iter => iter.map(x => Tuple2(x._1, 0.0))}
      , preservesPartitioning = true)

    predictions.persist(StorageLevel.MEMORY_AND_DISK)
    predictions.count()

    var treeId = 0
    while (forest.length < param.maxTreeNum) {
      LOG.info(s"train GBTree: $treeId")

      val (sampledFeatureBC, sampledSketchBC) = sampleFeature(sketch)
      // new tree
      val currentTree = createNewTree(treeId, instances)

      val gradHessRDD = calculateGradient(instances, predictions)

      while (currentTree.hasActiveNode) {
        LOG.info(s"growing tree, " + currentTree.toString)
        buildHistogram(currentTree, instances, gradHessRDD, sampledFeatureBC, sampledSketchBC)

        findSplit(currentTree, sampledFeatureBC, sampledSketchBC)

        growTree(currentTree, instances)
      }
      LOG.info(s"tree: $treeId grow finish, tree: ${currentTree.toString}")
      updateLeafPred(currentTree)
      predictions = updateInstancePred(currentTree, predictions)

      forest.append(currentTree)

      println(s"tree: $treeId train set ${param.loss.evalMetric}: " +
        s"${gbtModel.evaluate(instances.sample(false, 0.1).map(_._2))} " +
        s"validate set ${param.loss.evalMetric}: ${gbtModel.evaluate(validateSet)}")

      sampledFeatureBC.unpersist()
      sampledSketchBC.unpersist()
      treeId += 1
    }
    gbtModel
  }


  private[gbt] def init(dataSet: RDD[Instance]): (RDD[(Long, Instance)], RDD[Instance]) = {
    val validateFraction = param.validateFraction
    val rdds = dataSet.randomSplit(Array(1.0 - validateFraction, validateFraction))
    val (trainSet, validateSet) = (rdds(0), rdds(1))

    val instances = trainSet.zipWithIndex()
      .map(x => (x._2, x._1))
      .partitionBy(new HashPartitioner(param.partitionNum))
      .cache()
    validateSet.cache()
    LOG.info(s"train set size: ${instances.count()} validate set size: ${validateSet.count()}")

    val maxInstNum = instances.mapPartitions(iter => Iterator.single(iter.length)).max()

    LOG.info(s"[init] instance max partition size: $maxInstNum")
    instanceLayoutMat = PSMatrix.dense(param.partitionNum, maxInstNum)

    (instances, validateSet)
  }


  /**
   * Create data sketch, push candidate split value to PS
   */
  private[gbt] def createSketch(instances: RDD[(Long, Instance)]): Array[Array[Double]] = {
    val sketch = splitWithYahooSketch(instances, param.splitNum)
//  fake sketch
//  sketch = Range(0, param.featureNum).toArray.map(_ => Array(-0.5, 0.5))
    sketch
  }

  /**
   * Sample feature
   * different tree will base on different feature.
   */
  private[gbt] def sampleFeature(
      sketch: Array[Array[Double]]): (Broadcast[Array[Int]], Broadcast[Array[Array[Double]]]) = {
    val feats = if (param.featureSampleRate < 1.0) {
      val rand = new Random()

      val sampledFeat = new ArrayBuffer[Int]
      (0 until param.featureNum).foreach { id =>
        if (rand.nextDouble() < param.featureSampleRate) {
          sampledFeat.append(id)
        }
      }

      val fixedSizeFeats = if (sampledFeat.length < param.sampledFeatNum) {
        val featSet = new scala.collection.mutable.HashSet[Int]()
        featSet ++= sampledFeat.toIterator
        while (sampledFeat.length < param.sampledFeatNum) {
          val f = rand.nextInt(param.featureNum)
          if (!featSet.contains(f)) {
            sampledFeat.append(f)
            featSet += f
          }
        }
        sampledFeat.sorted.toArray
      } else if (sampledFeat.length > param.sampledFeatNum) {
        val feats = sampledFeat.toArray
        Maths.shuffle(feats)
        feats.slice(0, param.sampledFeatNum).sorted
      } else {
        sampledFeat.toArray
      }

      fixedSizeFeats
    } else {
      (0 until param.featureNum).toArray
    }

    val sampledSketch = feats.map (fId => sketch(fId))

    val sc = SparkSession.builder().getOrCreate().sparkContext

    (sc.broadcast(feats), sc.broadcast(sampledSketch))
  }

  def createNewTree(treeId: Int, instances: RDD[(Long, Instance)]): Tree = {
    val currentTree = new Tree(treeId, this.param.maxDepth)

    val posInfoMat = this.instancePosInfoMat
    val instLayout = this.instanceLayoutMat
    val param = this.paramBC
    val tempRDD = instances.mapPartitionsWithIndex { case (pId, iter) =>
      val pos = Array.fill[Double](param.value.maxNodeNum * 2)(-1.0)
      pos(0) = 0.0
      pos(1) = iter.length.toDouble - 1

      posInfoMat.push(pId, pos)
      // TODO: init a array sequence
      val cols = instLayout.columns.toInt
      instLayout.push(pId, (0 until cols).map(_.toDouble).toArray)
      Iterator.empty
    }
    tempRDD.count()

    currentTree
  }

  /**
   * calculate Gradient and Hessian
   */
  def calculateGradient(
      instances: RDD[(Long, Instance)],
      prediction: RDD[(Long, Double)]): RDD[(Long, (Double, Double))] = {

    val loss = this.param.loss
    val instanceGradRDD = instances.zip(prediction)
        .map { case ((instId1, instance), (instId2, pred)) =>
          require(instId1 == instId2)
          val transPred = loss.transPred(pred.toFloat)
          val grad = loss.firstGradient(instance.label.toFloat, transPred)
          val hess = loss.secondGradient(instance.label.toFloat, transPred)
          (instId1, (grad.toDouble, hess.toDouble))
        }
    instanceGradRDD.cache()
    instanceGradRDD.count()
    instanceGradRDD
  }


  private def resetHistogram(gradHist: DensePSMatrix): DensePSMatrix = {
    if (gradHist != null) gradHist.destroy()
    val conf = SparkSession.builder().getOrCreate().sparkContext.getConf
    val psNum = conf.get("spark.ps.instances", "1").toInt
    val psCore = conf.get("spark.ps.cores", "1").toInt
    val totalCore = psNum * psCore
    val samplesPerCore = math.ceil(param.sampledFeatNum.toDouble / totalCore).toInt

    DensePSMatrix(param.maxNodeNum, 2 * param.splitNum * param.sampledFeatNum,
      param.maxNodeNum, samplesPerCore * 2 * param.splitNum)
  }


  /**
   * Gradient and Hessian Histogram
   * There is a matrix for each node.
   * matrix(1, 2 * splitNum * sampleFeatNum)
   *
   * matrix num == node num
   * multi-thread to speed up
   */
  private[gbt] def buildHistogram(
      tree: Tree,
      instances: RDD[(Long, Instance)],
      gradRDD: RDD[(Long, (Double, Double))],
      sampledFeatBC: Broadcast[Array[Int]],
      sketchBC: Broadcast[Array[Array[Double]]]): Unit = {

    gradHistMatrix = resetHistogram(gradHistMatrix)

    val posInfoMat = this.instancePosInfoMat
    val layoutMat = this.instanceLayoutMat
    val gradHistMat = this.gradHistMatrix
    val gbtParam = this.paramBC
    println(tree.toString)
    val tempRDD = instances.zip(gradRDD)
      .mapPartitionsWithIndex { case (pId, iter) =>
        val instance2Grad = iter.map { case ((instId1, inst), (instId2, grad)) =>
          require(instId1 == instId2)
          (instId1, inst, grad)
        }.toArray
        val nodePos = posInfoMat.pull(pId)
        val instanceLayout = layoutMat.pull(pId)
        val zeroIndex = sketchBC.value.map { sketch => Utils.findFValuePosition(sketch, 0.0) }

        tree.forActive { node =>
          val nodeStart = nodePos(node.id * 2).toInt
          val nodeEnd = nodePos(node.id * 2 + 1).toInt

          if (nodeStart != -1 && nodeEnd != -1) {
            val histogram = new Array[Double](gbtParam.value.sampledFeatNum * 2 * gbtParam.value.splitNum)

            var gradSum = 0.0
            var hessSum = 0.0
            (nodeStart to nodeEnd).foreach { id =>
              val instance = instance2Grad(instanceLayout(id).toInt)._2
              val (grad, hess) = instance2Grad(instanceLayout(id).toInt)._3

              gradSum += grad
              hessSum += hess

              instance.feature.foreachActive { case (fId, fValue) =>
                val fPos = Utils.findFIdPosition(sampledFeatBC.value, fId)
                if (fPos != -1) {
                  val fValueIndex =
                    Utils.findFValuePosition(sketchBC.value(fPos), fValue)

                  val gradIndex = 2 * fPos * gbtParam.value.splitNum + fValueIndex
                  val hessIndex = (2 * fPos + 1) * gbtParam.value.splitNum + fValueIndex
                  histogram(gradIndex) += grad
                  histogram(hessIndex) += hess

                  val fZeroValueIndex = zeroIndex(fPos)
                  val zeroGradIndex = 2 * fPos * gbtParam.value.splitNum + fZeroValueIndex
                  val zeroHessIndex = (2 * fPos + 1) * gbtParam.value.splitNum + fZeroValueIndex
                  histogram(zeroGradIndex) -= grad
                  histogram(zeroHessIndex) -= hess
                } else {
                  // this feat has not been sampled.
                  println(s"fId: $fId is not sampled.")
                }
              }
            }

            sampledFeatBC.value.foreach { fId =>
              val fPos = Utils.findFIdPosition(sampledFeatBC.value, fId)
              val fZeroValueIndex = zeroIndex(fPos)
              val zeroGradIndex = 2 * fPos * gbtParam.value.splitNum + fZeroValueIndex
              val zeroHessIndex = (2 * fPos + 1) * gbtParam.value.splitNum + fZeroValueIndex
              histogram(zeroGradIndex) += gradSum
              histogram(zeroHessIndex) += hessSum
            }

            gradHistMat.increment(node.id, histogram)
          }
        }
        Iterator.empty
      }
    tempRDD.count()
  }

  def findSplit(
      tree: Tree,
      sampledFeatureBC: Broadcast[Array[Int]],
      sampledSketchBC: Broadcast[Array[Array[Double]]]): Unit = {

    val activeNodes = tree.getActiveNode.map(node => node.id)
    val thisParam = paramBC
    val gradHistMat = this.gradHistMatrix

    val spark = SparkSession.builder().getOrCreate()
    val splitEntries = spark.sparkContext.parallelize(activeNodes, activeNodes.length)
      .mapPartitions { iter =>
        val nId = iter.next()
        require(!iter.hasNext, "each partition must only have one element")
        val matrixId = gradHistMat.id
        val func = new GBDTGradHistGetRowFunc(new HistAggrParam(matrixId, nId,
          thisParam.value.splitNum, thisParam.value.minChildWeight.toFloat,
          thisParam.value.regAlpha.toFloat, thisParam.value.regLambda.toFloat))

        val angelEntry = gradHistMat.aggregate(func)
          .asInstanceOf[GBDTGradHistGetRowResult].getSplitEntry
        val splitEntry = SplitEntry.fromAngel(angelEntry)

        if (splitEntry.fId != -1) {
          val trueSplitFid = sampledFeatureBC.value(splitEntry.fId)
          val splitIndex = splitEntry.fValue.toInt + 1
          val trueSplitValue = sampledSketchBC.value(splitEntry.fId)(splitIndex)

          splitEntry.fId = trueSplitFid
          splitEntry.fValue = trueSplitValue

          println(s"find best split for node($nId) split entry is ${splitEntry.toString}")
          Iterator.single(Tuple2(nId, splitEntry))
        } else {
          println(s"failed to find a split for node($nId)")
          Iterator.empty
        }
      }.collect()

    // update the split entry
    val (splitIds, splitValues, gains, childGH) = SplitEntry.merge(splitEntries, param.maxNodeNum)

    LOG.info(s"find split feature: ${splitIds.mkString(" ")}")
    LOG.info(s"find split value: ${splitValues.mkString(" ")}")

    // push split entries to PS
    gbtModel.updateSplitFeature(tree.id, splitIds)
    gbtModel.updateSplitValue(tree.id, splitValues)
    splitGainMat.increment(tree.id, gains)
    gradHessMat.increment(tree.id, childGH)
  }

  private def updateInstancePred(tree: Tree, prediction: RDD[(Long, Double)]): RDD[(Long, Double)] = {

    val posInfoMat = this.instancePosInfoMat
    val layoutMat = this.instanceLayoutMat
    val thisParam = this.paramBC

    val thisPredictions = prediction.mapPartitionsWithIndex{ case (pId, iter) =>
      val predicts = iter.toArray

      val instancePos = posInfoMat.pull(pId).map(_.toInt)
      val layout = layoutMat.pull(pId).map(_.toInt)

      tree.foreachNode { node =>
        if (node.isLeaf) {
          val weight = node.leafValue
          val start = instancePos(2 * node.id)
          val end = instancePos(2 * node.id + 1)
          if (start != -1 && end != -1) {
            (start to end).foreach { i =>
              val instIndex = layout(i)
              val w = predicts(instIndex)._2 + thisParam.value.learningRate * weight
              predicts(instIndex) = Tuple2(predicts(instIndex)._1, w)
            }
          }
        }
      }
      predicts.toIterator
    }

    thisPredictions.persist(StorageLevel.MEMORY_AND_DISK)
    thisPredictions.count()
    prediction.unpersist()

    thisPredictions
  }

  private def updateLeafPred(tree: Tree): Unit = {
    val nodeWeight = new Array[Double](param.maxNodeNum)
    tree.foreachNode { node =>
      if (node.isLeaf) {
        nodeWeight(node.id) = node.leafValue
      }
    }
    gbtModel.updateLeafWeight(tree.id, nodeWeight)
  }

  private[gbt] def growTree(tree: Tree, instances: RDD[(Long, Instance)]): Unit = {
    val splitFeature = gbtModel.getSplitFeature(tree.id)
    val nodesGradHess = gradHessMat.pull(tree.id)

    LOG.info(s"before grow, tree: ${tree.toString}")

    // Update instance layout and position info
    updateInstanceLayout(tree, instances)
    tree.getActiveNode.foreach { node =>
      if (splitFeature(node.id) != -1) {
        tree.addChildren(node.id)
        val leftId = 2 * node.id + 1
        val rightId = 2 * node.id + 2

        if (tree.depth >= param.maxDepth) {
          // set child node to leaf
          val leftGradStats = new GradStats(nodesGradHess(leftId * 2), nodesGradHess(leftId * 2 + 1))
          val rightGradStats = new GradStats(nodesGradHess(rightId * 2), nodesGradHess(rightId * 2 + 1))

          tree.setLeaf(leftId, leftGradStats.calcWeight(param))
          tree.setLeaf(rightId, rightGradStats.calcWeight(param))
        } else {
          tree.setNodeActive(leftId)
          tree.setNodeActive(rightId)
        }
      } else {
        // set node to leaf, and set node inactive
        val gradStats = new GradStats(nodesGradHess(node.id * 2), nodesGradHess(node.id * 2 + 1))
        tree.setLeaf(node.id, gradStats.calcWeight(param))
      }
      tree.setNodeInactive(node.id)
    }
    LOG.info(s"after grow, tree: ${tree.toString}")
  }

  private def updateInstanceLayout(
      tree: Tree,
      instances: RDD[(Long, Instance)]): Unit = {

    val maxDepth = paramBC.value.maxDepth

    val posInfoMat = this.instancePosInfoMat
    val layoutMat = this.instanceLayoutMat
    val gbtModel = this.gbtModel

    val tempRDD = instances.mapPartitionsWithIndex { case (pId, iter) =>
      val dataSet = iter.toArray
      val splitFeatures = gbtModel.getSplitFeature(tree.id)
      val splitValues = gbtModel.getSplitValue(tree.id)

      val instancePos = posInfoMat.pull(pId).map(_.toInt)
      val layout = layoutMat.pull(pId).map(_.toInt)

      import scala.util.control._
      tree.forActive { node =>
        val nodeId = node.id
        val splitFeature = splitFeatures(nodeId)
        val splitValue = splitValues(nodeId)

        if (splitFeature != -1 && tree.depth < maxDepth) {

          val startPos = instancePos(nodeId * 2)
          val endPos = instancePos(nodeId * 2 + 1)
          if (startPos != -1 && endPos != -1) { // no valid instances in this partition
            var left = startPos
            var right = endPos

            while (left < right) {
              // 1. left to right, find the first instance that should be in the right child
              val loop = new Breaks
              loop.breakable {
                while (left < right) {
                  val leftInstIdx = layout(left)
                  val leftInstValue = dataSet(leftInstIdx)._2.feature(splitFeature)
                  if (leftInstValue > splitValue) loop.break()
                  left += 1
                }
              }

              // 2. right to left, find the first instance that should be in the left child
              loop.breakable {
                while (left < right) {
                  val rightInstIdx = layout(right)
                  val rightInstValue = dataSet(rightInstIdx)._2.feature(splitFeature)
                  if (rightInstValue <= splitValue) loop.break()
                  right -= 1
                }
              }

              // 3. swap two instances
              if (left < right) {
                val temp = layout(left)
                layout(left) = layout(right)
                layout(right) = temp
              }
            }
            require(left == right, s"left == right, but left is $left right is $right, start is $startPos end is $endPos")

            val currInstIdx = layout(left)
            val currValue = dataSet(currInstIdx)._2.feature(splitFeature)
            // the first instance that is larger than the split value
            val cutPos = if (currValue > splitValue) left - 1 else left

            if (startPos <= cutPos) {
              instancePos((2 * nodeId + 1) * 2) = startPos
              instancePos((2 * nodeId + 1) * 2 + 1) = cutPos
            }

            if (cutPos + 1 <= endPos) {
              instancePos((2 * nodeId + 2) * 2) = cutPos + 1
              instancePos((2 * nodeId + 2) * 2 + 1) = endPos
            }

            posInfoMat.push(pId, instancePos.map(_.toDouble))
            layoutMat.push(pId, layout.map(_.toDouble))
          }
        }
      }
      Iterator.empty
    }
    tempRDD.count()
  }

  private def splitWithYahooSketch(
      instances: RDD[(Long, Instance)],
      splitNum: Int): Array[Array[Double]] = {

    val featureNum = param.featureNum
    val defaultValue = param.defaultValue
    val partitionNum = instances.partitions.length
    val featNumPerPart = math.ceil(featureNum / partitionNum.toDouble).toInt

    // sample some data to create sketch.
    val sketches = instances.mapPartitionsWithIndex { (pId, iter) =>
      val thisFeatNum = if (featureNum >= (pId + 1) * featNumPerPart) {
        featNumPerPart
      } else if (featureNum > pId * featNumPerPart ) {
        featureNum - pId * featNumPerPart
      } else {
        0
      }

      if (thisFeatNum != 0) {
        val sketches = Array.fill(thisFeatNum)(DoublesSketch.builder().build())
        sketches.foreach(_.update(defaultValue))
        iter.foreach { case (id, point) =>
          point.feature.foreachActive { case (index, value) =>
            if (index >= pId * featNumPerPart && index < pId * featNumPerPart + thisFeatNum) {
              val sketchIdx = index - pId * featNumPerPart
              sketches(sketchIdx).update(value)
            }
          }
        }
        val fraction = (0 until splitNum).toArray.map(i => i.toDouble / splitNum)
        val quantiles = sketches.map { sk => sk.getQuantiles(fraction) }
          .map { quan =>
            (1 until quan.length).reverse.foreach { i => quan(i) = quan(i - 1) }
            quan
          }
        quantiles.toIterator
      } else {
        Iterator.empty
      }
    }.collect()

    require(sketches.length == featureNum,
      s"sketch length: ${sketches.length} feature num: $featureNum")
    sketches
  }

  /**
    *
    * @param modelPath model path.
    * @return
    */
  override def loadModel(modelPath: String): MLModel = ???
}
