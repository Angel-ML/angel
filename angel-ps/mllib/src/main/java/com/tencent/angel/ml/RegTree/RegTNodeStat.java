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

import com.tencent.angel.ml.param.RegTParam;
import com.tencent.angel.ml.tree.SplitEntry;

import java.util.List;


public class RegTNodeStat {

  public RegTParam param;
  public float lossChg; // loss change caused by current split
  public float sumGrad; // sum of gradient
  public float sumHess; // sum of hessian values, used to measure coverage of data
  public float baseWeight; // weight of current node
  public SplitEntry splitEntry;

  public RegTNodeStat() {}

  public RegTNodeStat(RegTParam param) {
    this.param = param;
    this.lossChg = 0.0f;
    this.sumGrad = 0.0f;
    this.sumHess = 0.0f;
    this.baseWeight = 0.0f;
    this.splitEntry = new SplitEntry();
  }

  public RegTNodeStat(RegTParam param, List<GradPair> gradPairs) {
    new RegTNodeStat(param);
    // calculate the sum of gradient and hess
    for (GradPair pair : gradPairs) {
      this.sumGrad += pair.getGrad();
      this.sumHess += pair.getHess();
    }
    this.baseWeight = param.calcWeight(sumGrad, sumHess);
    this.splitEntry = new SplitEntry();
  }

  public void setStats(GradStats gradStats) {
    this.sumGrad = gradStats.sumGrad;
    this.sumHess = gradStats.sumHess;
    this.baseWeight = gradStats.calcWeight(param);
  }

  public void setStats(float sumGrad, float sumHess) {
    this.sumGrad = sumGrad;
    this.sumHess = sumHess;
    this.baseWeight = this.param.calcWeight(sumGrad, sumHess);
  }

  public void setLossChg(float lossChg) {
    this.lossChg = lossChg;
  }

  public void setSplitEntry(SplitEntry splitEntry) {
    this.splitEntry = splitEntry;
  }
}
