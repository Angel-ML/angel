/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.ml.tree.gbdt.learner

import java.{util => ju}

import com.tencent.angel.spark.ml.tree.data.DataSet
import com.tencent.angel.spark.ml.tree.gbdt.histogram._
import com.tencent.angel.spark.ml.tree.gbdt.metadata.{DataInfo, FeatureInfo}
import com.tencent.angel.spark.ml.tree.gbdt.tree.{GBTNode, GBTSplit, GBTTree}
import com.tencent.angel.spark.ml.tree.objective.ObjectiveFactory
import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.split.SplitEntry
import com.tencent.angel.spark.ml.tree.util.{EvenPartitioner, Maths, RangeBitSet}
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Random

class FPGBDTLearner(val learnerId: Int,
                    val param: GBDTParam,
                    _featureInfo: FeatureInfo,
                    _trainData: DataSet,
                    _labels: Array[Float],
                    _validData: Array[Vector], _validLabel: Array[Float]) {

  @transient private[learner] val forest = ArrayBuffer[GBTTree]()

  @transient private val trainData: DataSet = _trainData
  @transient private val labels: Array[Float] = _labels
  @transient private val validData: Array[Vector] = _validData
  @transient private val validLabels: Array[Float] = _validLabel
  @transient private val validPreds = {
    if (param.numClass == 2)
      new Array[Float](validData.length)
    else
      new Array[Float](validData.length * param.numClass)
  }

  private[learner] val (featLo, featHi) = {
    val featureEdges = new EvenPartitioner(param.numFeature, param.numWorker).partitionEdges()
    (featureEdges(learnerId), featureEdges(learnerId + 1))
  }
  private[learner] val numFeatUsed = Math.round((featHi - featLo) * param.featSampleRatio)
  private[learner] val isFeatUsed = {
    if (numFeatUsed == featHi - featLo)
      (featLo until featHi).map(fid => _featureInfo.getNumBin(fid) > 0).toArray
    else
      new Array[Boolean](featHi - featLo)
  }
  private[learner] val featureInfo: FeatureInfo = _featureInfo
  private[learner] val dataInfo = DataInfo(param, labels.length)

  private[learner] val loss = ObjectiveFactory.getLoss(param.lossFunc)
  private[learner] val evalMetrics = ObjectiveFactory.getEvalMetricsOrDefault(param.evalMetrics, loss)

  // histograms and global best splits, one for each internal tree node
  private[learner] val storedHists = new Array[Array[Histogram]](Maths.pow(2, param.maxDepth) - 1)
  private[learner] val bestSplits = new Array[GBTSplit](Maths.pow(2, param.maxDepth) - 1)
  private[learner] val histBuilder = new HistBuilder(param)
  private[learner] val splitFinder = new SplitFinder(param)

  private[learner] val activeNodes = ArrayBuffer[Int]()

  private[learner] val buildHistTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  private[learner] val histSubtractTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  private[learner] val findSplitTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  private[learner] val getSplitResultTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  private[learner] val splitNodeTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)

  def timing[A](f: => A)(t: Long => Any): A = {
    val t0 = System.currentTimeMillis()
    val res = f
    t(System.currentTimeMillis() - t0)
    res
  }

  def reportTime(): String = {
    val sb = new StringBuilder
    for (depth <- 0 until param.maxDepth) {
      val from = Maths.pow(2, depth) - 1
      val until = Maths.pow(2, depth + 1) - 1
      if (from < Maths.pow(2, param.maxDepth) - 1) {
        sb.append(s"Layer${depth + 1}:\n")
        sb.append(s"|buildHistTime: [${buildHistTime.slice(from, until).mkString(", ")}], " +
          s"sum[${buildHistTime.slice(from, until).sum}]\n")
        sb.append(s"|histSubtractTime: [${histSubtractTime.slice(from, until).mkString(", ")}], " +
          s"sum[${histSubtractTime.slice(from, until).sum}]\n")
        sb.append(s"|findSplitTime: [${findSplitTime.slice(from, until).mkString(", ")}], " +
          s"sum[${findSplitTime.slice(from, until).sum}]\n")
        sb.append(s"|getSplitResultTime: [${getSplitResultTime.slice(from, until).mkString(", ")}], " +
          s"sum[${getSplitResultTime.slice(from, until).sum}]\n")
        sb.append(s"|splitNodeTime: [${splitNodeTime.slice(from, until).mkString(", ")}], " +
          s"sum[${splitNodeTime.slice(from, until).sum}]\n")
      }
    }
    val res = sb.toString()
    println(res)
    for (i <- buildHistTime.indices) {
      buildHistTime(i) = 0
      histSubtractTime(i) = 0
      findSplitTime(i) = 0
      getSplitResultTime(i) = 0
      splitNodeTime(i) = 0
    }
    res
  }

  def createNewTree(): Unit = {
    // 1. create new tree
    val tree = new GBTTree(param)
    this.forest += tree
    // 2. sample features
    if (numFeatUsed != featHi - featLo) {
      ju.Arrays.fill(isFeatUsed, false)
      for (_ <- 0 until numFeatUsed) {
        val rand = Random.nextInt(featHi - featLo)
        isFeatUsed(rand) = featureInfo.getNumBin(featLo + rand) > 0
      }
    }
    // 3. reset position info
    dataInfo.resetPosInfo()
    // 4. calc grads
    val sumGradPair = dataInfo.calcGradPairs(0, labels, loss, param)
    tree.getRoot.setSumGradPair(sumGradPair)
    // 5. set root status
    activeNodes += 0
  }

  def findSplits(): Seq[(Int, GBTSplit)] = {
    val res = if (activeNodes.nonEmpty) {
      buildHistAndFindSplit(activeNodes)
    } else {
      Seq.empty
    }
    activeNodes.clear()
    res
  }

  def getSplitResults(splits: Seq[(Int, GBTSplit)]): Seq[(Int, RangeBitSet)] = {
    val tree = forest.last
    splits.map {
      case (nid, split) =>
        tree.getNode(nid).setSplitEntry(split.getSplitEntry)
        bestSplits(nid) = split
        (nid, getSplitResult(nid, split.getSplitEntry))
    }.filter(_._2 != null)
  }

  def splitNodes(splitResults: Seq[(Int, RangeBitSet)]): Boolean = {
    splitResults.foreach {
      case (nid, result) =>
        splitNode(nid, result, bestSplits(nid))
        if (2 * nid + 1 < Maths.pow(2, param.maxDepth) - 1) {
          activeNodes += 2 * nid + 1
          activeNodes += 2 * nid + 2
        }
    }
    activeNodes.nonEmpty
  }

  def buildHistAndFindSplit(nids: Seq[Int]): Seq[(Int, GBTSplit)] = {
    val nodes = nids.map(forest.last.getNode)
    val sumGradPairs = nodes.map(_.getSumGradPair)
    val canSplits = nodes.map(canSplitNode)

    val buildStart = System.currentTimeMillis()
    var cur = 0
    while (cur < nids.length) {
      val nid = nids(cur)
      val sibNid = Maths.sibling(nid)
      if (cur + 1 < nids.length && nids(cur + 1) == sibNid) {
        if (canSplits(cur) || canSplits(cur + 1)) {
          val curSize = dataInfo.getNodeSize(nid)
          val sibSize = dataInfo.getNodeSize(sibNid)
          val parNid = Maths.parent(nid)
          val parHist = storedHists(parNid)
          if (curSize < sibSize) {
            timing {
              storedHists(nid) = histBuilder.buildHistogramsFP(
                isFeatUsed, featLo, trainData, featureInfo, dataInfo,
                nid, sumGradPairs(cur), parHist
              )
              storedHists(sibNid) = parHist
            } { t => buildHistTime(nid) = t }
            //            timing(storedHists(nid) = histBuilder.buildHistogramsFP(
            //              isFeatUsed, featLo, trainData, featureInfo, dataInfo,
            //              nid, sumGradPairs(cur)
            //            )) {t => buildHistTime(nid) = t}
            //            timing(storedHists(sibNid) = histBuilder.histSubtraction(
            //              parHist, storedHists(nid), true
            //            )) {t => histSubtractTime(sibNid) = t}
          } else {
            timing {
              storedHists(sibNid) = histBuilder.buildHistogramsFP(
                isFeatUsed, featLo, trainData, featureInfo, dataInfo,
                sibNid, sumGradPairs(cur + 1), parHist
              )
              storedHists(nid) = parHist
            } { t => buildHistTime(sibNid) = t }
            //            timing(storedHists(sibNid) = histBuilder.buildHistogramsFP(
            //              isFeatUsed, featLo, trainData, featureInfo, dataInfo,
            //              sibNid, sumGradPairs(cur + 1)
            //            )) {t => histSubtractTime(sibNid) = t}
            //            timing(storedHists(nid) = histBuilder.histSubtraction(
            //              parHist, storedHists(sibNid), true
            //            )) {t => buildHistTime(nid) = t}
          }
          storedHists(parNid) = null
        }
        cur += 2
      } else {
        if (canSplits(cur)) {
          timing(storedHists(nid) = histBuilder.buildHistogramsFP(
            isFeatUsed, featLo, trainData, featureInfo, dataInfo,
            nid, sumGradPairs(cur), null
          )) { t => buildHistTime(nid) = t }
        }
        cur += 1
      }
    }
    println(s"Build histograms cost ${System.currentTimeMillis() - buildStart} ms")

    val findStart = System.currentTimeMillis()
    val res = canSplits.zipWithIndex.map {
      case (canSplit, i) =>
        val nid = nids(i)
        timing(if (canSplit) {
          val node = nodes(i)
          val hist = storedHists(nid)
          val sumGradPair = sumGradPairs(i)
          val nodeGain = node.calcGain(param)
          val split = splitFinder.findBestSplitFP(featLo, hist,
            featureInfo, sumGradPair, nodeGain)
          (nid, split)
        } else {
          (nid, new GBTSplit)
        }) { t => findSplitTime(nid) = t }
    }.filter(_._2.isValid(param.minSplitGain))
    println(s"Find splits cost ${System.currentTimeMillis() - findStart} ms")
    res
  }

  def getSplitResult(nid: Int, splitEntry: SplitEntry): RangeBitSet = {
    require(!splitEntry.isEmpty && splitEntry.getGain > param.minSplitGain)
    //forest.last.getNode(nid).setSplitEntry(splitEntry)
    val splitFid = splitEntry.getFid
    if (featLo <= splitFid && splitFid < featHi) {
      val splits = featureInfo.getSplits(splitFid)
      timing(dataInfo.getSplitResult(nid, splitEntry, splits, trainData)) { t => getSplitResultTime(nid) = t }
    } else {
      null
    }
  }

  def splitNode(nid: Int, splitResult: RangeBitSet, split: GBTSplit = null): Unit = {
    timing {
      dataInfo.updatePos(nid, splitResult)
      val tree = forest.last
      val node = tree.getNode(nid)
      val leftChild = new GBTNode(2 * nid + 1, node, param.numClass)
      val rightChild = new GBTNode(2 * nid + 2, node, param.numClass)
      node.setLeftChild(leftChild)
      node.setRightChild(rightChild)
      tree.setNode(2 * nid + 1, leftChild)
      tree.setNode(2 * nid + 2, rightChild)
      if (split == null) {
        val leftSize = dataInfo.getNodeSize(2 * nid + 1)
        val rightSize = dataInfo.getNodeSize(2 * nid + 2)
        if (leftSize < rightSize) {
          val leftSumGradPair = dataInfo.sumGradPair(2 * nid + 1)
          val rightSumGradPair = node.getSumGradPair.subtract(leftSumGradPair)
          leftChild.setSumGradPair(leftSumGradPair)
          rightChild.setSumGradPair(rightSumGradPair)
        } else {
          val rightSumGradPair = dataInfo.sumGradPair(2 * nid + 2)
          val leftSumGradPair = node.getSumGradPair.subtract(rightSumGradPair)
          leftChild.setSumGradPair(leftSumGradPair)
          rightChild.setSumGradPair(rightSumGradPair)
        }
      } else {
        leftChild.setSumGradPair(split.getLeftGradPair)
        rightChild.setSumGradPair(split.getRightGradPair)
      }
    } { t => splitNodeTime(nid) = t }
  }

  def canSplitNode(node: GBTNode): Boolean = {
    if (dataInfo.getNodeSize(node.getNid) > param.minNodeInstance) {
      if (param.numClass == 2) {
        val sumGradPair = node.getSumGradPair.asInstanceOf[BinaryGradPair]
        param.satisfyWeight(sumGradPair.getGrad, sumGradPair.getHess)
      } else {
        val sumGradPair = node.getSumGradPair.asInstanceOf[MultiGradPair]
        param.satisfyWeight(sumGradPair.getGrad, sumGradPair.getHess)
      }
    } else {
      false
    }
  }

  def setAsLeaf(nid: Int): Unit = setAsLeaf(nid, forest.last.getNode(nid))

  def setAsLeaf(nid: Int, node: GBTNode): Unit = {
    node.chgToLeaf()
    if (param.numClass == 2) {
      val weight = node.calcWeight(param)
      dataInfo.updatePreds(nid, weight, param.learningRate)
    } else {
      val weights = node.calcWeights(param)
      dataInfo.updatePreds(nid, weights, param.learningRate)
    }
  }

  def finishTree(): Unit = {
    forest.last.getNodes.asScala.foreach {
      case (nid, node) =>
        if (node.getSplitEntry == null && !node.isLeaf)
          setAsLeaf(nid, node)
    }
    for (i <- storedHists.indices)
      storedHists(i) = null
  }

  def evaluate(): Seq[(EvalMetric.Kind, Double, Double)] = {
    for (i <- validData.indices) {
      var node = forest.last.getRoot
      while (!node.isLeaf) {
        if (node.getSplitEntry.flowTo(validData(i)) == 0)
          node = node.getLeftChild.asInstanceOf[GBTNode]
        else
          node = node.getRightChild.asInstanceOf[GBTNode]
      }
      if (param.numClass == 2) {
        validPreds(i) += node.getWeight * param.learningRate
      } else {
        val weights = node.getWeights
        for (k <- 0 until param.numClass)
          validPreds(i * param.numClass + k) += weights(k) * param.learningRate
      }
    }

    val metrics = evalMetrics.map(evalMetric =>
      (evalMetric.getKind, evalMetric.eval(dataInfo.predictions, labels),
        evalMetric.eval(validPreds, validLabels))
    )

    val evalTrainMsg = metrics.map(metric => s"${metric._1}[${metric._2}]").mkString(", ")
    println(s"Evaluation on train data after ${forest.size} tree(s): $evalTrainMsg")
    val evalValidMsg = metrics.map(metric => s"${metric._1}[${metric._3}]").mkString(", ")
    println(s"Evaluation on valid data after ${forest.size} tree(s): $evalValidMsg")
    metrics
  }

  def finalizeModel(): Seq[GBTTree] = {
    histBuilder.shutdown()
    forest
  }

}
