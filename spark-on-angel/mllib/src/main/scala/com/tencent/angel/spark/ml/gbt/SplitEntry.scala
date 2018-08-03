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

package com.tencent.angel.spark.ml.gbt

import com.tencent.angel.ml.GBDT.algo.tree.{SplitEntry => AngelEntry}

/**
 * Description: split info of a tree node
 */
class SplitEntry(
    var fId: Int = -1,
    var fValue: Double = 0.0,
    var gain: Double = 0.0) extends Serializable {

  // grad stats of the left child
  var leftGradStat: GradStats = new GradStats()
  // grad stats of the right child
  var rightGradStat: GradStats = new GradStats()

  /**
   * Update the split entry, replace it if e is better.
   *
   * @param e candidate split solution
   * @return the boolean whether the proposed split is better and can replace current split
   */
  def update(e: SplitEntry): Boolean = {
    if (betterSplit(e.gain, e.fId)) {
      this.gain = e.gain
      this.fId = e.fId
      this.fValue = e.fValue
      this.leftGradStat = e.leftGradStat
      this.rightGradStat = e.rightGradStat
      true
    } else {
      false
    }
  }

  def update(newLossChg: Double, splitFeature: Int, splitValue: Double, defaultLeft: Boolean): Boolean = {
    if (this.betterSplit(newLossChg, splitFeature)) {
      this.gain = newLossChg
      this.fId = if (defaultLeft) {
        splitFeature | (1 << 31)
      } else {
        splitFeature
      }
      this.fValue = splitValue
      true
    } else {
      false
    }
  }

  def update(newLossChg: Double, splitFeature: Int, splitValue: Double): Boolean = {
    if (this.betterSplit(newLossChg, splitFeature)) {
      this.gain = newLossChg
      this.fId = splitFeature
      this.fValue = splitValue
      true
    } else {
      false
    }
  }

  /**
   * whether missing value goes to left branch.
   *
   * @return the boolean
   */
  // TODO: how to deal with the missing value
  def defaultLeft(): Boolean = (fId >> 31) != 0

  // TODO: fix this logic
  private def betterSplit(newLossChg: Double, splitFeature: Double): Boolean = {
    if (this.fId <= splitFeature) {
      newLossChg > this.gain
    } else {
      newLossChg >= this.gain
    }
  }

  override def toString: String = {
    s"SplitEntry(fId: $fId fValue: $fValue gain: $gain " +
      s"leftChild: ${leftGradStat.toString} rightChild: ${rightGradStat.toString})"
  }
}

object SplitEntry {
  def merge(
      entries: Array[(Int, SplitEntry)],
      maxNodes: Int): (Array[Int], Array[Double], Array[Double], Array[Double]) = {

    val featureIds = Array.fill[Int](maxNodes)(-1)
    val splitValues = new Array[Double](maxNodes)
    val gains = new Array[Double](maxNodes)
    val childGradHess = new Array[Double](2 * maxNodes)

    entries.foreach { case (fId, entry) =>
      featureIds(fId) = entry.fId
      splitValues(fId) = entry.fValue
      gains(fId) = entry.gain

      val leftId = 2 * fId + 1
      val rightId = 2 * fId + 2

      childGradHess(2 * leftId) = entry.leftGradStat.sumGrad
      childGradHess(2 * leftId + 1) = entry.leftGradStat.sumHess
      childGradHess(2 * rightId) = entry.rightGradStat.sumGrad
      childGradHess(2 * rightId + 1) = entry.rightGradStat.sumHess
    }
    Tuple4(featureIds, splitValues, gains, childGradHess)
  }

  def fromAngel(entry: AngelEntry): SplitEntry = {
    val thisEntry = new SplitEntry(entry.fid, entry.fvalue, entry.lossChg)

    if (entry.leftGradStat != null) {
      val leftStat = new GradStats(entry.leftGradStat.sumGrad, entry.leftGradStat.sumHess)
      thisEntry.leftGradStat = leftStat
    }

    if (entry.rightGradStat != null) {
      val rightStat = new GradStats(entry.rightGradStat.sumGrad, entry.rightGradStat.sumHess)
      thisEntry.rightGradStat = rightStat
    }
    thisEntry
  }
}
