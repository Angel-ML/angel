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
package com.tencent.angel.ml.RegTree;

import com.tencent.angel.ml.param.RegTTrainParam;
import com.tencent.angel.ml.tree.SplitEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Description: a single histogram of a feature
 */

public class HistUnit {

  private static final Log LOG = LogFactory.getLog(HistUnit.class);

  public int nid;
  public int fid;
  private RegTTrainParam param;
  private float[] cut; // cutting point of histogram, contains the maximum point as the last one
  private GradStats[] data; // content of statistics data
  private int size; // size of histogram

  public HistUnit(int fid, RegTTrainParam param) {
    this.fid = fid;
    this.param = param;
    this.size = param.numSplit + 1; // binnumber = splitnumber +1
    this.cut = new float[param.numSplit];
    this.data = new GradStats[size];
  }

  public HistUnit(int nid, int fid, RegTTrainParam param, float[] cut) {
    this.nid = nid;
    this.fid = fid;
    this.param = param;
    this.size = param.numSplit + 1;
    assert this.size == cut.length + 1;
    this.cut = cut;
    this.data = new GradStats[size];
    for (int i = 0; i < size; i++) {
      data[i] = new GradStats();
    }
  }


  public HistUnit(int fid, int size, float[] cut, GradStats[] data) {
    assert cut.length == size && data.length == size + 1;
    this.fid = fid;
    this.size = size;
    this.cut = cut;
    this.data = data;
  }

  public float[] getCut() {
    return cut;
  }

  public void setCut(float[] cut) {
    this.cut = cut;
  }

  public GradStats[] getData() {
    return data;
  }

  public void setData(GradStats[] data) {
    this.data = data;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  /**
   * add a histogram to data.
   *
   * @param fv the value of feature
   * @param gpair the gpair list
   * @param dataMeta the data info
   * @param ridx the instances index in gpair
   */
  public void add(float fv, List<GradPair> gpair, DataMeta dataMeta, int ridx) {
    int i = findPlace(fv);
    assert i < size;
    data[i].add(gpair, dataMeta, ridx);
  }

  public void add(int fid, float fv, GradPair gpair) {
    assert fid == this.fid;
    add(fv, gpair);
  }

  public void add(float fv, GradPair gpair) {
    int i = findPlace(fv);
    assert i < size;
    data[i].add(gpair);
    // LOG.info(String.format("Add fid[%d] fvalue[%f] to [%d]-bin", this.fid, fv, i));
  }

  private int findPlace(float value) {
    int rec = 0;
    if (value <= cut[0])
      return rec;
    else
      rec++;
    for (; rec < cut.length - 1; rec++) {
      if (value > cut[rec - 1] && value <= cut[rec]) {
        return rec;
      }
    }
    return rec;
  }

  public SplitEntry findBestSplit(RegTNodeStat nodeStat, SplitEntry splitEntry,
                                  GradStats bestLeftStat, GradStats bestRightStat) {
    GradStats rootStats = new GradStats(nodeStat.sumGrad, nodeStat.sumHess);
    float rootGain = rootStats.calcGain(param);
    LOG.info(String.format("Node[%d] fid[%d]: sumGrad[%f], sumHess[%f], gain[%f]", this.nid,
        this.fid, nodeStat.sumGrad, nodeStat.sumHess, rootGain));
    GradStats leftStats = new GradStats();
    GradStats rightStats = new GradStats();
    // loop over all the data in hist
    for (int i = 0; i < data.length; i++) {
      leftStats.add(data[i]);
      // whether we can split with current hessian
      if (leftStats.sumHess >= param.minChildWeight) {
        // right = root -left
        rightStats.setSubstract(rootStats, leftStats);
        // whether we can split with current hessian
        if (rightStats.sumHess >= param.minChildWeight) {
          float lossChg = leftStats.calcGain(param) + rightStats.calcGain(param) - rootGain;
          // LOG.info(String.format("Left child of node[%d]: sumGrad[%f], sumHess[%f]; " +
          // "right child of node[%d]: sumGrad[%f], sumHess[%f]; lossChg[%f]",
          // this.nid, leftStats.sumGrad, leftStats.sumHess,
          // this.nid, rightStats.sumGrad, rightStats.sumHess, lossChg));
          if (splitEntry.update(lossChg, fid, cut[i])) {
            LOG.info(String.format("Find new best split: fid[%d], fvalue[%f], lossChg[%f]",
                splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
            bestLeftStat.update(leftStats.sumGrad, leftStats.sumHess);
            bestRightStat.update(rightStats.sumGrad, rightStats.sumHess);
          }
        }
      }
    }
    nodeStat.setLossChg(splitEntry.lossChg);
    nodeStat.setSplitEntry(splitEntry);
    // LOG.info(String.format("The best split of node[%d]: fid[%d], fvalue[%f], lossChg[%f]",
    // this.nid, splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
    return splitEntry;
  }

  @Override
  public String toString() {
    String rec =
        String.format("[-inf-%f]:[%f][%f], ", this.cut[0], this.data[0].sumGrad,
            this.data[0].sumHess);
    int i = 1;
    for (; i < cut.length; i++) {
      rec +=
          String.format("[%f-%f]:[%f][%f], ", this.cut[i - 1], this.cut[i], this.data[i].sumGrad,
              this.data[i].sumHess);
    }
    rec +=
        String.format("[%f-+inf]:[%f][%f]", this.cut[i - 1], this.data[i].sumGrad,
            this.data[i].sumHess);
    return rec;
  }
}
