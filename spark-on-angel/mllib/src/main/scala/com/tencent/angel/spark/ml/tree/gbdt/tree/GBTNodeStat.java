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


import com.tencent.angel.spark.ml.tree.basic.TNodeStat;

public class GBTNodeStat extends TNodeStat {
    /*private GradPair sumGradPair;

    public GBTNodeStat(GradPair sumGradPair) {
        this.sumGradPair = sumGradPair;
    }

    public float calcWeight(GBDTParam param) {
        nodeWeight = param.calcWeight(sumGrad, sumHess);
        return nodeWeight;
    }

    public float calcGain(GBDTParam param) {
        gain = param.calcGain(sumGrad, sumHess);
        return gain;
    }

    public float getSumGrad() {
        return sumGrad;
    }

    public float getSumHess() {
        return sumHess;
    }

    public void setSumGrad(float sumGrad) {
        this.sumGrad = sumGrad;
    }

    public void setSumHess(float sumHess) {
        this.sumHess = sumHess;
    }*/
}
