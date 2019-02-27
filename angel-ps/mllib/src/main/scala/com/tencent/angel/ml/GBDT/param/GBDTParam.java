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


package com.tencent.angel.ml.GBDT.param;

import com.tencent.angel.ml.GBDT.metric.EvalMetric;
import com.tencent.angel.ml.GBDT.metric.LogErrorMetric;
import com.tencent.angel.ml.GBDT.metric.RMSEMetric;
import com.tencent.angel.ml.GBDT.objective.Loss;
import com.tencent.angel.ml.GBDT.objective.RegLossObj;

public class GBDTParam extends RegTParam {

  public int treeNum = 10;
  public int maxThreadNum = 20;
  public int batchSize = 100000;

  // task type: classification, regression, or ranking
  public String taskType;

  // quantile sketch, size = featureNum * splitNum
  public String sketchName;
  // gradient histograms, size = treeNodeNum * featureNum * splitNum
  public String gradHistNamePrefix;
  // active tree nodes, size = pow(2, treeDepth) -1
  public String activeTreeNodesName;
  // sampled features. size = treeNum * sampleRatio * featureNum
  public String sampledFeaturesName;
  // categorical feature. size = workerNum * cateFeatNum * splitNum
  public String cateFeatureName;
  // split features, size = treeNum * treeNodeNum
  public String splitFeaturesName;
  // split values, size = treeNum * treeNodeNum
  public String splitValuesName;
  // split gains, size = treeNum * treeNodeNum
  public String splitGainsName;
  // node weights, size = treeNum * treeNodeNum
  public String nodeGradStatsName;
  // node preds, size = treeNum * treeNodeNum
  public String nodePredsName;

  // if using PS to perform split
  public boolean isServerSplit = true;

  public RegLossObj getLossFunc() {
    switch (taskType) {
      case "classification":
        return new RegLossObj(new Loss.BinaryLogisticLoss());
      case "regression":
        return new RegLossObj(new Loss.LinearSquareLoss());
      default:
        return new RegLossObj(new Loss.BinaryLogisticLoss());
    }
  }

  public EvalMetric getEvalMetric() {
    switch (taskType) {
      case "classification":
        return new LogErrorMetric();
      case "regression":
        return new RMSEMetric();
      default:
        return new LogErrorMetric();
    }
  }

}