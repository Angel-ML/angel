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


package com.tencent.angel.spark.ml.tree.gbdt.tree;

import com.tencent.angel.spark.ml.tree.gbdt.histogram.BinaryGradPair;
import com.tencent.angel.spark.ml.tree.gbdt.histogram.GradPair;
import com.tencent.angel.spark.ml.tree.gbdt.histogram.MultiGradPair;
import com.tencent.angel.spark.ml.tree.basic.TNode;
import com.tencent.angel.spark.ml.tree.param.GBDTParam;


public class GBTNode extends TNode<GBTNodeStat> {

  private GradPair sumGradPair;
  private float gain;
  private float[] weights;

  public GBTNode(int nid, TNode parent, int numClass) {
    this(nid, parent, null, null, numClass);
  }

  public GBTNode(int nid, TNode parent, TNode left, TNode right, int numClass) {
    super(nid, parent, left, right);
    //this.nodeStats = new GBTNodeStat[numClass == 2 ? 1 : numClass];
  }

  public GradPair getSumGradPair() {
    return sumGradPair;
  }

  public void setSumGradPair(GradPair sumGradPair) {
    this.sumGradPair = sumGradPair;
  }

  public float calcGain(GBDTParam param) {
    gain = sumGradPair.calcGain(param);
    return gain;
  }

  public float calcWeight(GBDTParam param) {
    if (weights == null) {
      weights = new float[1];
    }
    weights[0] = ((BinaryGradPair) sumGradPair).calcWeight(param);
    return weights[0];
  }

  public float[] calcWeights(GBDTParam param) {
    weights = ((MultiGradPair) sumGradPair).calcWeights(param);
    return weights;
  }

  public float getWeight() {
    return weights[0];
  }

  public float[] getWeights() {
    return weights;
  }

    /*public void setGradStats(float sumGrad, float sumHess) {
        this.nodeStats[0] = new GBTNodeStat(sumGrad, sumHess);
    }

    public void setGradStats(float[] sumGrad, float[] sumHess) {
        for (int i = 0; i < this.nodeStats.length; i++) {
            this.nodeStats[i] = new GBTNodeStat(sumGrad[i], sumHess[i]);
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
    }*/
}
