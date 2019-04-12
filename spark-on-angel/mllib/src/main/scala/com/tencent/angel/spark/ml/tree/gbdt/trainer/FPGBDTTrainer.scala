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


package com.tencent.angel.spark.ml.tree.gbdt.trainer

import java.util.concurrent.Executors
import java.{util => ju}

import com.tencent.angel.spark.ml.tree.gbdt.dataset.Dataset
import com.tencent.angel.spark.ml.tree.gbdt.helper.{HistManager, SplitFinder}
import com.tencent.angel.spark.ml.tree.gbdt.histogram.{BinaryGradPair, GradPair, MultiGradPair}
import com.tencent.angel.spark.ml.tree.gbdt.metadata.{FeatureInfo, InstanceInfo}
import com.tencent.angel.spark.ml.tree.gbdt.tree.{GBTNode, GBTSplit, GBTTree}
import com.tencent.angel.spark.ml.tree.objective.ObjectiveFactory
import com.tencent.angel.spark.ml.tree.objective.loss.{Loss, MultiStrategy}
import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric
import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric.Kind
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.split.{SplitEntry, SplitPoint}
import com.tencent.angel.spark.ml.tree.util.{Maths, RangeBitSet}
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Random

class FPGBDTTrainer(val workerId: Int, val param: GBDTParam,
                    @transient private[gbdt] val featureInfo: FeatureInfo,
                    @transient private[gbdt] val trainData: Dataset[Int, Int],
                    @transient private[gbdt] val labels: Array[Float],
                    @transient private[gbdt] val validData: Array[Vector],
                    @transient private[gbdt] val validLabels: Array[Float]) extends Serializable {
  private[gbdt] val forest = ArrayBuffer[GBTTree]()

  @transient private[gbdt] val validPreds = {
    if (param.numClass == 2)
      new Array[Float](validData.length)
    else
      new Array[Float](validData.length * param.numClass)
  }

  private[gbdt] val instanceInfo = InstanceInfo(param, labels.length)
  private[gbdt] val numFeatUsed = Math.round(featureInfo.numFeature * param.featSampleRatio)
  private[gbdt] val isFeatUsed = {
    if (numFeatUsed == featureInfo.numFeature)
      (0 until numFeatUsed).map(fid => featureInfo.getNumBin(fid) > 0).toArray
    else
      new Array[Boolean](numFeatUsed)
  }

  private[gbdt] val activeNodes = ArrayBuffer[Int]()

  @transient private[gbdt] val histManager = HistManager(param, featureInfo)
  @transient private[gbdt] val splitFinder = SplitFinder(param, featureInfo)
  @transient private[gbdt] val threadPool = if (param.numThread > 1)
    Executors.newFixedThreadPool(param.numThread) else null

  @transient private[gbdt] val buildHistTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  @transient private[gbdt] val findSplitTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  @transient private[gbdt] val getSplitResultTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)
  @transient private[gbdt] val splitNodeTime = new Array[Long](Maths.pow(2, param.maxDepth) - 1)

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
    if (numFeatUsed != featureInfo.numFeature) {
      ju.Arrays.fill(isFeatUsed, false)
      for (_ <- 0 until numFeatUsed) {
        val rand = Random.nextInt(featureInfo.numFeature)
        isFeatUsed(rand) = featureInfo.getNumBin(rand) > 0
      }
      histManager.reset(isFeatUsed)
    } else if (forest.length == 1) {
      histManager.reset(isFeatUsed)
    }
    // 3. reset position info
    instanceInfo.resetPosInfo()
    // 4. calc grads
    val loss = getLoss
    val sumGradPair =
      if (param.isMultiClassMultiTree) {
        instanceInfo.calcGradPairsForMultiClassMultiTree(labels, loss, param, forest.size, threadPool)
      } else {
        instanceInfo.calcGradPairs(labels, loss, param, threadPool)
      }
    tree.getRoot.setSumGradPair(sumGradPair)
    histManager.setGradPair(0, sumGradPair)
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

  def getSplitResults(splits: Seq[(Int, Int, Int, GBTSplit)]): Seq[(Int, RangeBitSet)] = {
    val tree = forest.last
    splits.flatMap {
      case (nid, ownerId, fidInWorker, split) =>
        tree.getNode(nid).setSplitEntry(split.getSplitEntry)
        histManager.setGradPair(2 * nid + 1, split.getLeftGradPair)
        histManager.setGradPair(2 * nid + 2, split.getRightGradPair)
        if (ownerId == this.workerId)
          Iterator((nid, getSplitResult(nid, fidInWorker, split.getSplitEntry)))
        else
          Iterator.empty
    }
  }

  def splitNodes(splitResults: Seq[(Int, RangeBitSet)]): Boolean = {
    splitResults.foreach {
      case (nid, result) =>
        splitNode(nid, result, (histManager.getGradPair(2 * nid + 1),
          histManager.getGradPair(2 * nid + 2)))
        if (2 * nid + 1 < Maths.pow(2, param.maxDepth) - 1) {
          activeNodes += 2 * nid + 1
          activeNodes += 2 * nid + 2
        } else {
          histManager.removeNodeHist(nid)
        }
    }
    activeNodes.nonEmpty
  }

  /**
    *
    * @param nids: active tree nodes
    * @return
    */
  def buildHistAndFindSplit(nids: Seq[Int]): Seq[(Int, GBTSplit)] = {
    val canSplits = nids.map(canSplitNode)

    val buildStart = System.currentTimeMillis()
    var cur = 0
    val toBuild = ArrayBuffer[Int]()
    val toSubtract = ArrayBuffer[Boolean]()
    while (cur < nids.length) {
      val nid = nids(cur)
      val sibNid = Maths.sibling(nid)
      if (cur + 1 < nids.length && nids(cur + 1) == sibNid) {
        if (canSplits(cur) || canSplits(cur + 1)) {
          val curSize = instanceInfo.getNodeSize(nid)
          val sibSize = instanceInfo.getNodeSize(sibNid)
          if (curSize < sibSize) {
            toBuild += nid
            toSubtract += canSplits(cur + 1)
          } else {
            toBuild += sibNid
            toSubtract += canSplits(cur)
          }
        } else {
          histManager.removeNodeHist(Maths.parent(nid))
        }
        cur += 2
      } else {
        if (canSplits(cur)) {
          toBuild += nid
          toSubtract += false
        }
        cur += 1
      }
    }
    if (toBuild.nonEmpty) {
      timing {
        if (toBuild.head == 0) {
          histManager.buildHistForRoot(trainData, instanceInfo, threadPool)
        } else {
          histManager.buildHistForNodes(toBuild, trainData, instanceInfo, toSubtract, threadPool)
        }
      } { t => buildHistTime(nids.min) = t }
    }
    println(s"Build histograms cost ${System.currentTimeMillis() - buildStart} ms")

    val findStart = System.currentTimeMillis()

    val res = (nids, canSplits).zipped.map {
      case (nid, true) =>
        val hist = histManager.getNodeHist(nid)
        val sumGradPair = histManager.getGradPair(nid)
        val nodeGain = forest.last.getNode(nid).calcGain(param)
        val split = timing {
          splitFinder.findBestSplit(hist, sumGradPair, nodeGain)
        } { t => findSplitTime(nid) = t }
        (nid, if (split.isValid(param.minSplitGain)) split else new GBTSplit())
      case (nid, false) =>
        setAsLeaf(nid)
        (nid, new GBTSplit())
    }.filter(_._2.isValid(param.minSplitGain))
    println(s"Find splits cost ${System.currentTimeMillis() - findStart} ms")
    res.foreach { case (nid, split) =>
      println(s"node[$nid] split ${split.getSplitEntry.toString}")
    }
    res
  }

  def getSplitResult(nid: Int, fidInWorker: Int, splitEntry: SplitEntry): RangeBitSet = {
    require(!splitEntry.isEmpty && splitEntry.getGain > param.minSplitGain)
    val splits = featureInfo.getSplits(fidInWorker)
    timing(instanceInfo.getSplitResult(nid, fidInWorker, splitEntry, splits, trainData, threadPool)) {
      t => getSplitResultTime(nid) = t
    }
  }

  def splitNode(nid: Int, splitResult: RangeBitSet,
                childrenGradPairs: (GradPair, GradPair)): Unit = {
    timing {
      instanceInfo.updatePos(nid, splitResult)
      val tree = forest.last
      val node = tree.getNode(nid)
      val leftChild = new GBTNode(2 * nid + 1, node, param.numClass)
      val rightChild = new GBTNode(2 * nid + 2, node, param.numClass)
      node.setLeftChild(leftChild)
      node.setRightChild(rightChild)
      tree.setNode(2 * nid + 1, leftChild)
      tree.setNode(2 * nid + 2, rightChild)
      leftChild.setSumGradPair(childrenGradPairs._1)
      rightChild.setSumGradPair(childrenGradPairs._2)
    } { t => splitNodeTime(nid) = t }
  }

  def canSplitNode(nid: Int): Boolean = {
    if (instanceInfo.getNodeSize(nid) > param.minNodeInstance) {
      if (param.numClass == 2 || param.isMultiClassMultiTree) {
        val sumGradPair = histManager.getGradPair(nid).asInstanceOf[BinaryGradPair]
        param.satisfyWeight(sumGradPair.getGrad, sumGradPair.getHess)
      } else {
        val sumGradPair = histManager.getGradPair(nid).asInstanceOf[MultiGradPair]
        param.satisfyWeight(sumGradPair.getGrad, sumGradPair.getHess)
      }
    } else {
      false
    }
  }

  def setAsLeaf(nid: Int): Unit = setAsLeaf(nid, forest.last.getNode(nid))

  def setAsLeaf(nid: Int, node: GBTNode): Unit = {
    node.chgToLeaf()
    // TODO: update predictions of all training instance together
    // If not multi-class multi-tree
    if (param.isMultiClassMultiTree) {
      val weight = node.calcWeight(param)
      val curClass = (forest.size - 1) % param.numClass
      instanceInfo.updatePredsForMultiClassMultiTree(nid, curClass, weight, param.learningRate)
    } else {
      if (param.numClass == 2) {
        val weight = node.calcWeight(param)
        instanceInfo.updatePreds(nid, weight, param.learningRate)
      } else {
        val weights = node.calcWeights(param)
        instanceInfo.updatePreds(nid, weights, param.learningRate)
      }
    }
    if (nid < Maths.pow(2, param.maxDepth) - 1) // node not on the last level
      histManager.removeNodeHist(nid)
  }

  def finishTree(): Unit = {
    forest.last.getNodes.asScala.foreach {
      case (nid, node) =>
        if (node.getSplitEntry == null && !node.isLeaf)
          setAsLeaf(nid, node)
    }
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
        if (param.isMultiClassMultiTree) {
          val k = (forest.size - 1) % param.numClass
          validPreds(i * param.numClass + k) += node.getWeight * param.learningRate
        } else {
          val weights = node.getWeights
          for (k <- 0 until param.numClass)
            validPreds(i * param.numClass + k) += weights(k) * param.learningRate
        }
      }
    }

    val slice = Maths.avgSlice(labels.length, param.numWorker, workerId)
    val evalMetrics = getEvalMetrics
    val metrics = evalMetrics.map(evalMetric => {
      val kind = evalMetric.getKind
      val trainSum = evalMetric.sum(instanceInfo.predictions, labels, slice._1, slice._2)
      val trainMetric = kind match {
        case Kind.AUC => evalMetric.avg(trainSum, 1)
        case _ => evalMetric.avg(trainSum, slice._2 - slice._1)
      }
      val validSum = evalMetric.sum(validPreds, validLabels)
      val validMetric = kind match {
        case Kind.AUC => evalMetric.avg(validSum, 1)
        case _ => evalMetric.avg(validSum, validLabels.length)
      }
      (kind, trainSum, trainMetric, validSum, validMetric)
    })

    if (! (param.isMultiClassMultiTree && forest.size % param.numClass != 0) ) {
      val round = if (param.isMultiClassMultiTree) forest.size / param.numClass else forest.size
      val evalTrainMsg = metrics.map(metric => s"${metric._1}[${metric._3}]").mkString(", ")
      println(s"Evaluation on train data after ${round} tree(s): $evalTrainMsg")
      val evalValidMsg = metrics.map(metric => s"${metric._1}[${metric._5}]").mkString(", ")
      println(s"Evaluation on valid data after ${round} tree(s): $evalValidMsg")
    }

    metrics.map(metric => (metric._1, metric._2, metric._4))
  }

  def finalizeModel(): Seq[GBTTree] = {
    println(s"Worker[$workerId] finalizing...")
    if (threadPool != null) threadPool.shutdown()
    forest
  }

  def getLoss: Loss = ObjectiveFactory.getLoss(param.lossFunc)

  def getEvalMetrics: Array[EvalMetric] = ObjectiveFactory.getEvalMetricsOrDefault(param.evalMetrics, getLoss)

}
