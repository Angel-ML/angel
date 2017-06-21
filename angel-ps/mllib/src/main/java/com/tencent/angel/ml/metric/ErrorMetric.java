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
 * Description: the metric of error ratio
 */

public class ErrorMetric implements EvalMetric {

  private static float threshold = 0.5f;

  /**
   * return name of metric
   *
   * @return the name
   */
  @Override
  public String getName() {
    return "error";
  }

  /**
   * evaluate a specific metric for instances
   *
   * @param preds the predictions
   * @param labels the labels
   * @return the eval metric
   */
  @Override
  public float eval(float[] preds, float[] labels) {
    float err = 0.0f;
    for (int i = 0; i < preds.length; i++) {
      err += evalOne(preds[i], labels[i]);
    }
    return err / preds.length;
  }

  /**
   * evaluate a specific metric for one instance
   *
   * @param pred the prediction
   * @param label the label
   * @return the eval metric
   */
  @Override
  public float evalOne(float pred, float label) {
    return pred > threshold ? 1.0f - label : label;
  }
}
