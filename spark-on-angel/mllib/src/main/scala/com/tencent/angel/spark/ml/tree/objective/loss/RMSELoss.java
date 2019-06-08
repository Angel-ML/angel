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

package com.tencent.angel.spark.ml.tree.objective.loss;

import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric;
import java.util.Arrays;
import javax.inject.Singleton;
import com.tencent.angel.spark.ml.tree.util.Maths;

@Singleton
public class RMSELoss implements BinaryLoss, MultiLoss {

  private static RMSELoss instance;

  private RMSELoss() {
  }

  @Override
  public Kind getKind() {
    return Kind.RMSE;
  }

  @Override
  public EvalMetric.Kind defaultEvalMetric() {
    return EvalMetric.Kind.RMSE;
  }

  @Override
  public double firOrderGrad(float pred, float label) {
    return pred - label;
  }

  @Override
  public double secOrderGrad(float pred, float label) {
    return 1.0;
  }

  @Override
  public double secOrderGrad(float pred, float label, double firGrad) {
    return 1.0;
  }

  @Override
  public double[] firOrderGrad(float[] pred, float label) {
    int numLabel = pred.length;
    int trueLabel = (int) label;
    double[] grad = new double[numLabel];
    for (int i = 0; i < numLabel; i++) {
      grad[i] = pred[i] - (trueLabel == i ? 1 : 0);
    }
    return grad;
  }

  @Override
  public double[] secOrderGradDiag(float[] pred, float label) {
    int numLabel = pred.length;
    double[] hess = new double[numLabel];
    Arrays.fill(hess, 1.0);
    return hess;
  }

  @Override
  public double[] secOrderGradDiag(float[] pred, float label, double[] firGrad) {
    return secOrderGradDiag(pred, label);
  }

  @Override
  public double[] secOrderGradFull(float[] pred, float label) {
    int numLabel = pred.length;
    int size = (numLabel + 1) * numLabel / 2;
    double[] hess = new double[size];
    for (int i = 0; i < numLabel; i++) {
      int t = Maths.indexOfLowerTriangularMatrix(i, i);
      hess[t] = 1.0;
    }
    return hess;
  }

  @Override
  public double[] secOrderGradFull(float[] pred, float label, double[] firGrad) {
    return secOrderGradFull(pred, label);
  }

  public static RMSELoss getInstance() {
    if (instance == null) {
      instance = new RMSELoss();
    }
    return instance;
  }
}
