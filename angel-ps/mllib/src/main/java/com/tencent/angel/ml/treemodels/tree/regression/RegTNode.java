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

package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.basic.TNode;


public class RegTNode extends TNode<RegTNodeStat> {

  public RegTNode(int nid, TNode parent, int numClass) {
    this(nid, parent, null, null, numClass);
  }

  public RegTNode(int nid, TNode parent, TNode left, TNode right, int numClass) {
    super(nid, parent, left, right);
    this.nodeStats = new RegTNodeStat[numClass == 2 ? 1 : numClass];
  }

  public void setGradStats(float sumGrad, float sumHess) {
    this.nodeStats[0] = new RegTNodeStat(sumGrad, sumHess);
  }

  public void setGradStats(float[] sumGrad, float[] sumHess) {
    for (int i = 0; i < this.nodeStats.length; i++) {
      this.nodeStats[i] = new RegTNodeStat(sumGrad[i], sumHess[i]);
    }
  }

  public float[] calcWeight(GBDTParam param) {
    float[] nodeWeights = new float[nodeStats.length];
    for (int i = 0; i < nodeStats.length; i++) {
      nodeWeights[i] = nodeStats[i].calcWeight(param);
    }
    return nodeWeights;
  }

  public float[] calcGain(GBDTParam param) {
    float[] nodeGains = new float[nodeStats.length];
    for (int i = 0; i < nodeStats.length; i++) {
      nodeGains[i] = nodeStats[i].calcGain(param);
    }
    return nodeGains;
  }

}
