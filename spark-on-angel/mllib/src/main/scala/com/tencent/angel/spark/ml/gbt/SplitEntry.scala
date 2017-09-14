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

/**
 * Description: split info of a tree node
 */
class SplitEntry(var fid: Int = -1, var fvalue: Double = 0.0, var lossChg: Double = 0.0) {

  // grad stats of the left child
  var leftGradStat: GradStats = _
  // grad stats of the right child
  var rightGradStat: GradStats = _

  def needReplace(newLossChg: Double, splitFeature: Double): Boolean = {
    if (this.fid <= splitFeature) {
      newLossChg > this.lossChg
    } else {
      !(this.lossChg > newLossChg)
    }
  }

  /**
   * Update the split entry, replace it if e is better.
   *
   * @param e candidate split solution
   * @return the boolean whether the proposed split is better and can replace current split
   */
  def update(e: SplitEntry): Boolean = {
    if (needReplace(e.lossChg, e.fid)) {
      this.lossChg = e.lossChg
      this.fid = e.fid
      this.fvalue = e.fvalue
      this.leftGradStat = e.leftGradStat
      this.rightGradStat = e.rightGradStat
      true
    } else {
      false
    }
  }

  def update(newLossChg: Double, splitIdx: Int, newSplitValue: Double, defaultLeft: Boolean): Boolean = {
    if (this.needReplace(newLossChg, splitIdx)) {
      this.lossChg = newLossChg
      this.fid = if (defaultLeft) {
        splitIdx | (1 << 31)
      } else {
        splitIdx
      }
      this.fvalue = newSplitValue
      true
    } else {
      false
    }
  }

  def update(newLossChg: Double, splitFeature: Int, newSplitValue: Double): Boolean = {
    if (this.needReplace(newLossChg, splitFeature)) {
      this.lossChg = newLossChg
      this.fid = splitFeature
      this.fvalue = newSplitValue
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
  def defaultLeft(): Boolean = (fid >> 31) != 0

}
