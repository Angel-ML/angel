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


package com.tencent.angel.spark.ml.tree.gbdt.metadata

import com.tencent.angel.spark.ml.tree.gbdt.dataset.Dataset
import com.tencent.angel.spark.ml.tree.gbdt.histogram.{BinaryGradPair, GradPair, MultiGradPair}
import com.tencent.angel.spark.ml.tree.objective.loss._
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.split.SplitEntry
import com.tencent.angel.spark.ml.tree.util.{ConcurrentUtil, Maths, RangeBitSet}
import java.util.concurrent.ExecutorService


object InstanceInfo {

  def apply(param: GBDTParam, numData: Int): InstanceInfo = {
    val predSize = if (param.numClass == 2) numData else param.numClass * numData
    val gradSize = if (param.numClass == 2 || param.isMultiClassMultiTree) numData else predSize
    // 2 class: preds of each instance
    // multi-class one-tree: probs_ins_1, probs_ins_2, ...
    // multi-class multi-tree: probs_ins_1, probs_ins_2, ...
    val predictions = new Array[Float](predSize)
    val weights = Array.fill[Float](numData)(1.0f)
    val gradients = new Array[Double](gradSize)
    val hessians = new Array[Double](gradSize)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    val nodePosStart = new Array[Int](maxNodeNum)
    val nodePosEnd = new Array[Int](maxNodeNum)
    val nodeToIns = new Array[Int](numData)
    InstanceInfo(param, predictions, weights, gradients, hessians, nodePosStart, nodePosEnd, nodeToIns)
  }
}

case class InstanceInfo(param: GBDTParam, predictions: Array[Float], weights: Array[Float], gradients: Array[Double], hessians: Array[Double],
                        nodePosStart: Array[Int], nodePosEnd: Array[Int], nodeToIns: Array[Int]) {

  val gradCache: Array[Double] = if (param.isMultiClassMultiTree && param.multiGradCache) new Array[Double](predictions.size) else Array.empty
  val hessCache: Array[Double] = if (param.isMultiClassMultiTree && param.multiGradCache) new Array[Double](predictions.size) else Array.empty

  def resetPosInfo(): Unit = {
    val num = weights.length
    nodePosStart(0) = 0
    nodePosEnd(0) = num - 1
    for (i <- 0 until num) {
      nodeToIns(i) = i
    }
  }

  def calcGradPairs(labels: Array[Float], loss: Loss, param: GBDTParam,
                    threadPool: ExecutorService = null): GradPair = {
    def calcGP(start: Int, end: Int): GradPair = {
      val numClass = param.numClass
      if (numClass == 2) {
        // binary classification
        val binaryLoss = loss.asInstanceOf[BinaryLoss]
        var sumGrad = 0.0
        var sumHess = 0.0
        for (insId <- start until end) {
          val grad = binaryLoss.firOrderGrad(predictions(insId), labels(insId))
          val hess = binaryLoss.secOrderGrad(predictions(insId), labels(insId), grad)
          gradients(insId) = grad
          hessians(insId) = hess
          sumGrad += grad
          sumHess += hess
        }
        new BinaryGradPair(sumGrad, sumHess)
      } else if (!param.fullHessian) {
        // multi-label classification, assume hessian matrix is diagonal
        val multiLoss = loss.asInstanceOf[MultiLoss]
        val preds = new Array[Float](numClass)
        val sumGrad = new Array[Double](numClass)
        val sumHess = new Array[Double](numClass)
        for (insId <- start until end) {
          Array.copy(predictions, insId * numClass, preds, 0, numClass)
          val grad = multiLoss.firOrderGrad(preds, labels(insId))
          val hess = multiLoss.secOrderGradDiag(preds, labels(insId), grad)
          for (k <- 0 until numClass) {
            gradients(insId * numClass + k) = grad(k)
            hessians(insId * numClass + k) = hess(k)
            sumGrad(k) += grad(k)
            sumHess(k) += hess(k)
          }
        }
        new MultiGradPair(sumGrad, sumHess)
      } else {
        // multi-label classification, represent hessian matrix as lower triangular matrix
        throw new UnsupportedOperationException("Full hessian not supported")
      }
    }

    val numIns = labels.length
    if (param.numThread == 1) {
      calcGP(0, numIns)
    } else {
      ConcurrentUtil.rangeParallel(calcGP, 0, numIns, threadPool)
        .map(_.get())
        .reduceLeft((gp1, gp2) => {
          gp1.plusBy(gp2); gp1
        })
    }
  }

  def calcGradPairsForMultiClassMultiTree(labels: Array[Float], loss: Loss, param: GBDTParam, curTree: Int,
                                          threadPool: ExecutorService = null): GradPair = {
    def calcBinaryGPForMultiClassMultiTree(start: Int, end: Int): GradPair = {
      val numClass = param.numClass
      val curClass = (curTree - 1) % numClass   // class starts from 0
      val binaryLoss = loss.asInstanceOf[BinaryLoss]
      var sumGrad = 0.0
      var sumHess = 0.0
      for (insId <- start until end) {
        val predInd = numClass * insId + curClass
        val grad = binaryLoss.firOrderGrad(predictions(predInd), if (labels(insId) == curClass) 1 else 0)
        val hess = binaryLoss.secOrderGrad(predictions(predInd), if (labels(insId) == curClass) 1 else 0, grad)
        gradients(insId) = grad
        hessians(insId) = hess
        sumGrad += grad
        sumHess += hess
      }
      new BinaryGradPair(sumGrad, sumHess)
    }

    def calcGPForMultiClassMultiTree(start: Int, end: Int): GradPair = {
      val numClass = param.numClass
      val curClass = (curTree - 1) % numClass   // class starts from 0
      var sumGrad = 0.0
      var sumHess = 0.0
      for (insId <- start until end) {
        val pair = calcGradHess(predictions, labels, insId, curClass)
        gradients(insId) = pair._1
        hessians(insId) = pair._2
        sumGrad += pair._1
        sumHess += pair._2
      }
      new BinaryGradPair(sumGrad, sumHess)
    }

    def calcGradHess(preds: Array[Float], labels: Array[Float], insId: Int, curClass: Int): (Double, Double) = {
      val numClass = preds.length / labels.length
      val points = new Array[Float](numClass)
      Array.copy(preds, insId * numClass, points, 0, numClass)
      var wMax = 0.0
      for(point <- points) {
        wMax = math.max(wMax, point)
      }
      var wSum = 0.0
      for(point <- points) {
        wSum += math.exp(point - wMax)
      }
      var p = math.exp(points(curClass) - wMax) / wSum
      val h = math.max(2.0 * p * (1.0 - p), 1e-16)
      p = if (labels(insId) == curClass) p - 1.0 else p
      (p, h)
    }

    def calcGPWithCache(start: Int, end: Int): GradPair = {
      val numClass = param.numClass
      val curClass = (curTree - 1) % numClass
      var sumGrad = 0.0
      var sumHess = 0.0
      for (insId <- start until end) {
        val grad = gradCache(insId * numClass + curClass)
        val hess = hessCache(insId * numClass + curClass)
        gradients(insId) = grad
        hessians(insId) = hess
        sumGrad += grad
        sumHess += hess
      }
      new BinaryGradPair(sumGrad, sumHess)
    }

    def calcFullGP(start: Int, end: Int): Unit = {
      val numClass = param.numClass
      for (insId <- start until end) {
        val points = new Array[Float](numClass)
        Array.copy(predictions, insId * numClass, points, 0, numClass)
        var wMax = 0.0
        for(point <- points) {
          wMax = math.max(wMax, point)
        }
        var wSum = 0.0
        for(point <- points) {
          wSum += math.exp(point - wMax)
        }
        for (k <- 0 until numClass) {
          var p = math.exp(points(k) - wMax) / wSum
          val h = math.max(2.0 * p * (1.0 - p), 1e-16)
          p = if (labels(insId) == k) p - 1.0 else p
          gradCache(insId * numClass + k) = p
          hessCache(insId * numClass + k) = h
        }
      }
    }

    val numIns = labels.length
    val numClass = param.numClass
    val curClass = (curTree - 1) % numClass
    if (param.multiGradCache && curClass == 0) {
      calcFullGP(0, numIns)
    }
    if (param.numThread == 1) {
      if (param.multiGradCache)
        calcGPForMultiClassMultiTree(0, numIns)
      else
        calcGPWithCache(0, numIns)
    } else {
      if (param.multiGradCache)
        ConcurrentUtil.rangeParallel(calcGPWithCache, 0, numIns, threadPool)
          .map(_.get())
          .reduceLeft((gp1, gp2) => {
            gp1.plusBy(gp2); gp1
          })
      else
        ConcurrentUtil.rangeParallel(calcGPForMultiClassMultiTree, 0, numIns, threadPool)
        .map(_.get())
        .reduceLeft((gp1, gp2) => {
          gp1.plusBy(gp2); gp1
        })

    }
  }

  def getSplitResult(nid: Int, fidInWorker: Int, splitEntry: SplitEntry, splits: Array[Float],
                     dataset: Dataset[Int, Int], threadPool: ExecutorService = null): RangeBitSet = {
    val res = new RangeBitSet(nodePosStart(nid), nodePosEnd(nid))

    def split(start: Int, end: Int): Unit = {
      for (posId <- start until end) {
        val insId = nodeToIns(posId)
        val binId = dataset.get(insId, fidInWorker)
        val flowTo = if (binId >= 0) {
          splitEntry.flowTo(splits(binId))
        } else {
          splitEntry.defaultTo()
        }
        if (flowTo == 1)
          res.set(posId)
      }
    }

    if (threadPool == null) {
      split(nodePosStart(nid), nodePosEnd(nid) + 1)
    } else {
      ConcurrentUtil.rangeParallel(split, nodePosStart(nid), nodePosEnd(nid) + 1, threadPool)
        .foreach(_.get())
    }
    res
  }

  def updatePos(nid: Int, splitResult: RangeBitSet): Unit = {
    val nodeStart = nodePosStart(nid)
    val nodeEnd = nodePosEnd(nid)
    var left = nodeStart
    var right = nodeEnd
    while (left < right) {
      while (left < right && !splitResult.get(left)) left += 1
      while (left < right && splitResult.get(right)) right -= 1
      if (left < right) {
        val leftInsId = nodeToIns(left)
        val rightInsId = nodeToIns(right)
        nodeToIns(left) = rightInsId
        nodeToIns(right) = leftInsId
        left += 1
        right -= 1
      }
    }
    // find the cut position
    val cutPos = if (left == right) {
      if (splitResult.get(left)) left - 1
      else left
    } else {
      right
    }
    nodePosStart(2 * nid + 1) = nodeStart
    nodePosEnd(2 * nid + 1) = cutPos
    nodePosStart(2 * nid + 2) = cutPos + 1
    nodePosEnd(2 * nid + 2) = nodeEnd
  }

  def updatePreds(nid: Int, update: Float, learningRate: Float): Unit = {
    val update_ = update * learningRate
    val nodeStart = nodePosStart(nid)
    val nodeEnd = nodePosEnd(nid)
    for (i <- nodeStart to nodeEnd) {
      val insId = nodeToIns(i)
      predictions(insId) += update_
    }
  }

  def updatePredsForMultiClassMultiTree(nid: Int, curClass: Int, update: Float, learningRate: Float): Unit = {
    val update_ = update * learningRate
    val nodeStart = nodePosStart(nid)
    val nodeEnd = nodePosEnd(nid)
    for (i <- nodeStart to nodeEnd) {
      val insId = nodeToIns(i)
      val predInd = param.numClass * insId + curClass
      predictions(predInd) += update_
    }
  }

  def updatePreds(nid: Int, update: Array[Float], learningRate: Float): Unit = {
    val numClass = update.length
    val update_ = update.map(_ * learningRate)
    val nodeStart = nodePosStart(nid)
    val nodeEnd = nodePosEnd(nid)
    for (i <- nodeStart to nodeEnd) {
      val insId = nodeToIns(i)
      val offset = insId * numClass
      for (k <- 0 until numClass)
        predictions(offset + k) += update_(k)
    }
  }

  def numInstance: Int = nodeToIns.length

  def getNodePosStart(nid: Int) = nodePosStart(nid)

  def getNodePosEnd(nid: Int) = nodePosEnd(nid)

  def getNodeSize(nid: Int): Int = nodePosEnd(nid) - nodePosStart(nid) + 1

}
