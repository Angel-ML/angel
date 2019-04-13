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


package com.tencent.angel.spark.ml.tree.gbdt.helper

import com.tencent.angel.spark.ml.tree.gbdt.histogram.{BinaryGradPair, GradPair, Histogram, MultiGradPair}
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.gbdt.tree.GBTSplit
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.split.{SplitPoint, SplitSet}

import scala.collection.mutable.ArrayBuffer

object SplitFinder {

  def findBestSplitOfOneFeature(param: GBDTParam, fid: Int, featureInfo: FeatureInfo, histogram: Histogram,
                                sumGradPair: GradPair, nodeGain: Float): GBTSplit =
    findBestSplitOfOneFeature(param, fid, featureInfo.isCategorical(fid), featureInfo.getSplits(fid),
      featureInfo.getDefaultBin(fid), histogram, sumGradPair, nodeGain)

  def findBestSplitOfOneFeature(param: GBDTParam, fid: Int, isCategorical: Boolean,
                                splits: Array[Float], defaultBin: Int, histogram: Histogram,
                                sumGradPair: GradPair, nodeGain: Float): GBTSplit = {
    if (isCategorical) {
      findBestSplitSet(param, fid, splits, defaultBin, histogram, sumGradPair, nodeGain)
    } else {
      findBestSplitPoint(param, fid, splits, defaultBin, histogram, sumGradPair, nodeGain)
    }
  }

  def findBestSplitPoint(param: GBDTParam, fid: Int, splits: Array[Float], defaultBin: Int,
                         histogram: Histogram, sumGradPair: GradPair, nodeGain: Float): GBTSplit = {
    val splitPoint = new SplitPoint()
    val leftStat = if (param.numClass == 2 || param.isMultiClassMultiTree) {
      new BinaryGradPair()
    } else {
      new MultiGradPair(param.numClass, param.fullHessian)
    }
    val rightStat = sumGradPair.copy()
    var bestLeftStat = null.asInstanceOf[GradPair]
    var bestRightStat = null.asInstanceOf[GradPair]
    val numBin = histogram.getNumBin
    for (binId <- 0 until numBin - 1) {
      histogram.scan(binId, leftStat, rightStat)
      if (leftStat.satisfyWeight(param) && rightStat.satisfyWeight(param)) {
        val lossChg = leftStat.calcGain(param) + rightStat.calcGain(param) - nodeGain - param.regLambda
        if (splitPoint.needReplace(lossChg)) {
          splitPoint.setFid(fid)
          splitPoint.setFvalue(splits(binId + 1))
          splitPoint.setGain(lossChg)
          bestLeftStat = leftStat.copy()
          bestRightStat = rightStat.copy()
        }
      }
    }
    new GBTSplit(splitPoint, bestLeftStat, bestRightStat)
  }

  def findBestSplitSet(param: GBDTParam, fid: Int, splits: Array[Float], defaultBin: Int,
                       histogram: Histogram, sumGradPair: GradPair, nodeGain: Float): GBTSplit = {

    def binFlowTo(left: GradPair, bin: GradPair): Int = {
      if (param.numClass == 2 || param.isMultiClassMultiTree) {
        val sumGrad = sumGradPair.asInstanceOf[BinaryGradPair].getGrad
        val leftGrad = left.asInstanceOf[BinaryGradPair].getGrad
        val binGrad = bin.asInstanceOf[BinaryGradPair].getGrad
        if (binGrad * (2 * leftGrad + binGrad - sumGrad) >= 0.0) 0 else 1
      } else {
        val sumGrad = sumGradPair.asInstanceOf[MultiGradPair].getGrad
        val leftGrad = left.asInstanceOf[MultiGradPair].getGrad
        val binGrad = bin.asInstanceOf[MultiGradPair].getGrad
        var dot = 0.0
        for (i <- 0 until param.numClass)
          dot += binGrad(i) * (2 * leftGrad(i) + binGrad(i) - sumGrad(i))
        if (dot >= 0.0) 0 else 1
      }
    }

    // 1. set default bin to left child
    val leftStat = histogram.get(defaultBin).copy()
    // 2. for other bins, find its location
    var firstFlow = -1
    var curFlow = -1
    var curSplitId = 0
    val edges = ArrayBuffer[Float]()
    edges.sizeHint(FeatureInfo.ENUM_THRESHOLD)
    val binGradPair = if (param.numClass == 2 || param.isMultiClassMultiTree) {
      new BinaryGradPair()
    } else {
      new MultiGradPair(param.numClass, param.fullHessian)
    }
    val numBin = histogram.getNumBin
    for (binId <- 0 until numBin) {
      if (binId != defaultBin) { // skip default bin
        histogram.put(binId, binGradPair) // re-use
        val flowTo = binFlowTo(leftStat, binGradPair)
        if (flowTo == 0) leftStat.plusBy(binGradPair)
        if (firstFlow == -1) {
          firstFlow = flowTo
          curFlow = flowTo
        } else if (flowTo != curFlow) {
          edges += splits(curSplitId)
          curFlow = flowTo
        }
        curSplitId += 1
      }
    }
    // 3. create split set
    if (edges.size > 1 || curFlow != 0) { // whether all bins go the left
      val rightStat = sumGradPair.subtract(leftStat)
      if (leftStat.satisfyWeight(param) && rightStat.satisfyWeight(param)) {
        val lossChg = leftStat.calcGain(param) + rightStat.calcGain(param) - nodeGain - param.regLambda
        if (lossChg > 0.0f) {
          val splitSet = new SplitSet(fid, lossChg, edges.toArray, firstFlow, 0)
          return new GBTSplit(splitSet, leftStat, rightStat)
        }
      }
    }
    new GBTSplit
  }

  def apply(param: GBDTParam, featureInfo: FeatureInfo): SplitFinder =
    new SplitFinder(param, featureInfo)

}

import com.tencent.angel.spark.ml.tree.gbdt.helper.SplitFinder._

class SplitFinder(param: GBDTParam, featureInfo: FeatureInfo) {

  def findBestSplit(histograms: Array[Histogram], sumGradPair: GradPair,
                    nodeGain: Float): GBTSplit = {
    val best = new GBTSplit
    for (fid <- histograms.indices) {
      if (histograms(fid) != null) {
        val cur = findBestSplitOfOneFeature(param, fid, featureInfo,
          histograms(fid), sumGradPair, nodeGain)
        best.update(cur)
      }
    }
    best
  }

}
