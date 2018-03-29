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
package com.tencent.angel.ml.metric;

/**
 * Description: the metric of logistic loss
 */

public class LogLossMetric implements EvalMetric {

  /**
   * return name of metric
   *
   * @return the name
   */
  @Override public String getName() {
    return "logloss";
  }

  /**
   * evaluate a specific metric for instances
   *
   * @param predProbs the probability predictions
   * @param labels    the labels
   * @return the eval metric
   */
  @Override public float eval(float[] predProbs, float[] labels) {
    float errSum = 0.0f;
    for (int i = 0; i < predProbs.length; i++) {
      errSum += evalOne(predProbs[i], labels[i]);
    }
    return errSum / predProbs.length;
  }

  /**
   * evaluate a specific metric for one instance
   *
   * @param predProb the probability prediction
   * @param label    the label
   * @return the eval metric
   */
  @Override public float evalOne(float predProb, float label) {
    float eps = 1e-16f;
    float pneg = 1.0f - predProb;
    if (predProb < eps) {
      return -label * (float) Math.log(eps) - (1.0f - label) * (float) Math.log(1.0f - eps);
    } else if (pneg < eps) {
      return -label * (float) Math.log(1.0f - eps) - (1.0f - label) * (float) Math.log(eps);
    } else {
      return -label * (float) Math.log(predProb) - (1.0f - label) * (float) Math.log(pneg);
    }
  }


}
