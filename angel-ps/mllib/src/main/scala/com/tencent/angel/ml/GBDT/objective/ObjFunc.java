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


package com.tencent.angel.ml.GBDT.objective;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradPair;
import com.tencent.angel.ml.GBDT.algo.RegTree.RegTDataStore;

import java.util.List;

/**
 * interface of objective function Description:
 */

public interface ObjFunc {

  /**
   * get gradient over each of predictions, given existing information. preds: prediction of current
   * round info information about labels, weights, groups in rank iteration current iteration
   * number. return:_gpair output of get gradient, saves gradient and second order gradient in
   */
  GradPair[] calGrad(float[] preds, RegTDataStore info, int iteration);

  /**
   * transform prediction values, this is only called when Prediction is called preds: prediction
   * values, saves to this vector as well
   */
  void transPred(float[] preds);

  /**
   * transform prediction values, this is only called when Eval is called usually it redirect to
   * transPred preds: prediction values, saves to this vector as well
   */
  void transEval(float[] preds);

  /**
   * transform probability value back to margin this is used to transform user-set base_score back
   * to margin used by gradient boosting
   */
  float prob2Margin(float base_score);

  // return the default evaluation metric for the objective
  String defaultEvalMetric();
}