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
package com.tencent.angel.ml.objective;

import com.tencent.angel.ml.utils.MathUtils;

/**
 * Description: implementation of popular loss function
 *
 */

public class Loss {

  /**
   * Linear square loss loss. (y, y') = 1/2(y'-y)^2, grad = y'-y
   */
  public static class LinearSquareLoss implements LossHelper {

    public LinearSquareLoss() {}

    @Override
    public float transPred(float x) {
      return x;
    }

    @Override
    public boolean checkLabel(float x) {
      return true;
    }

    @Override
    public float firOrderGrad(float pred, float label) {
      return (pred - label);
    }

    @Override
    public float secOrderGrad(float pred, float label) {
      return 1.0f;
    }

    @Override
    public float prob2Margin(float baseScore) {
      return baseScore;
    }

    @Override
    public String labelErrorMsg() {
      return "";
    }

    @Override
    public String defaultEvalMetric() {
      return "rmse";
    }
  }

  /**
   * logistic loss for probability regression task. loss(y,y')=y*log(1+e^-y')+(1-y)*log(1+e^y'),
   * grad = y'-y, 2nd-grad = y'*(1-y')
   */
  public static class LogisticLoss implements LossHelper {

    public LogisticLoss() {}

    @Override
    public float transPred(float x) {
      return MathUtils.sigmoid(x);
    }

    @Override
    public boolean checkLabel(float x) {
      return x >= 0.0f && x <= 1.0f;
    }

    @Override
    public float firOrderGrad(float pred, float label) {
      return pred - label;
    }

    @Override
    public float secOrderGrad(float pred, float label) {
      float eps = 1e-16f;
      return Math.max(pred * (1 - pred), eps);
    }

    @Override
    public float prob2Margin(float baseScore) {
      assert baseScore > 0 && baseScore < 1.0f; // base_score must be in (0,1) for logistic loss
      return (float) Math.log(1.0 / (double) baseScore - 1.0);
    }

    @Override
    public String labelErrorMsg() {
      return "label must be in [0,1] for logistic regression";
    }

    @Override
    public String defaultEvalMetric() {
      return "rmse";
    }
  }

  // logistic loss for binary classification task.
  public static class BinaryLogisticLoss extends LogisticLoss {

    public BinaryLogisticLoss() {}

    @Override
    public String defaultEvalMetric() {
      return "error";
    }
  }

  // logistic loss, but predict un-transformed margin
  public static class DirectLogisticLoss extends LogisticLoss {

    public DirectLogisticLoss() {}

    @Override
    public float transPred(float x) {
      return x;
    }

    @Override
    public float firOrderGrad(float pred, float label) {
      pred = MathUtils.sigmoid(pred);
      return pred - label;
    }

    @Override
    public float secOrderGrad(float pred, float label) {
      float eps = 1e-16f;
      pred = MathUtils.sigmoid(pred);
      return Math.max(pred * (1 - pred), eps);
    }

    @Override
    public String defaultEvalMetric() {
      return "auc";
    }
  }

}
