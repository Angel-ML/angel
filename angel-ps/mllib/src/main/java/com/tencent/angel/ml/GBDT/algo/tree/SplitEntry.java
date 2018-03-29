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
 *
 */
package com.tencent.angel.ml.GBDT.algo.tree;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Description: split info of a tree node
 */

public class SplitEntry {

  private static final Log LOG = LogFactory.getLog(SplitEntry.class);

  // feature index used to split
  public int fid;
  // feature value used to split
  public float fvalue;
  // loss change after split this node
  public float lossChg;
  // grad stats of the left child
  public GradStats leftGradStat;
  // grad stats of the right child
  public GradStats rightGradStat;

  public SplitEntry(int fid, float fvalue, float lossChg) {
    // LOG.info("Constructor with fid = -1");
    this.fid = fid;
    this.fvalue = fvalue;
    this.lossChg = lossChg;
    this.leftGradStat = new GradStats();
    this.rightGradStat = new GradStats();
  }

  // fid = -1: no split currently
  public SplitEntry() {
    this(-1, (float) 0.0, (float) 0.0);
  }

  public void setFid(int fid) {
    this.fid = fid;
  }

  public void setFvalue(float fvalue) {
    this.fvalue = fvalue;
  }

  public void setLossChg(float lossChg) {
    this.lossChg = lossChg;
  }

  public int getFid() {
    return fid;
  }

  public float getFvalue() {
    return fvalue;
  }

  public float getLossChg() {
    return lossChg;
  }

  /**
   * decides whether we can replace current entry with the given statistics This function gives
   * better priority to lower index when loss_chg == new_loss_chg. Not the best way, but helps to
   * give consistent result during multi-thread execution.
   *
   * @param newLossChg   the new loss change
   * @param splitFeature the split index
   * @return the boolean whether the proposed split is better and can replace current split
   */
  public boolean NeedReplace(float newLossChg, int splitFeature) {
    if (this.fid <= splitFeature) {
      return newLossChg > this.lossChg;
    } else {
      return !(this.lossChg > newLossChg);
    }
  }

  /**
   * Update the split entry, replace it if e is better.
   *
   * @param e candidate split solution
   * @return the boolean whether the proposed split is better and can replace current split
   */
  public boolean update(SplitEntry e) {
    if (this.NeedReplace(e.lossChg, e.getFid())) {
      this.lossChg = e.lossChg;
      this.fid = e.fid;
      this.fvalue = e.fvalue;
      this.leftGradStat = e.leftGradStat;
      this.rightGradStat = e.rightGradStat;
      return true;
    } else {
      return false;
    }
  }

  public boolean update(float newLossChg, int splitIdx, float newSplitValue, boolean defaultLeft) {
    if (this.NeedReplace(newLossChg, splitIdx)) {
      this.lossChg = newLossChg;
      if (defaultLeft)
        splitIdx |= (1 << 31);
      this.fid = splitIdx;
      this.fvalue = newSplitValue;
      return true;
    } else {
      return false;
    }
  }

  public boolean update(float newLossChg, int splitFeature, float newSplitValue) {
    if (this.NeedReplace(newLossChg, splitFeature)) {
      this.lossChg = newLossChg;
      this.fid = splitFeature;
      this.fvalue = newSplitValue;
      return true;
    } else {
      return false;
    }
  }

  /**
   * whether missing value goes to left branch.
   *
   * @return the boolean
   */
  public boolean defaultLeft() {
    return (fid >> 31) != 0;
  }
}
