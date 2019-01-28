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
import com.tencent.angel.spark.ml.tree.util.Maths;
import javax.inject.Singleton;

@Singleton
public class MultinomialLogisticLoss implements MultiLoss {

  private static MultinomialLogisticLoss instance;

  private MultinomialLogisticLoss() {
  }

  @Override
  public Kind getKind() {
    return Kind.MultiLogistic;
  }

  @Override
  public EvalMetric.Kind defaultEvalMetric() {
    return EvalMetric.Kind.CROSS_ENTROPY;
  }

  @Override
  public double[] firOrderGrad(float[] pred, float label) {
    double[] prob = Maths.floatArrayToDoubleArray(pred);
    Maths.softmax(prob);
    int trueLabel = (int) label;
    double[] grad = prob;
    for (int i = 0; i < grad.length; i++) {
      grad[i] = (trueLabel == i ? prob[i] - 1.0 : prob[i]);
    }
    return grad;
  }

  @Override
  public double[] secOrderGradDiag(float[] pred, float label) {
    double[] prob = Maths.floatArrayToDoubleArray(pred);
    Maths.softmax(prob);
    double[] hess = prob;
    for (int i = 0; i < hess.length; i++) {
      hess[i] = Math.max(prob[i] * (1.0f - prob[i]), Maths.EPSILON);
    }
    return hess;
  }

  @Override
  public double[] secOrderGradDiag(float[] pred, float label, double[] firGrad) {
    int trueLabel = (int) label;
    double[] hess = new double[pred.length];
    for (int i = 0; i < hess.length; i++) {
      double prob = trueLabel == i ? firGrad[i] + 1.0 : firGrad[i];
      hess[i] = Math.max(prob * (1.0 - prob), Maths.EPSILON);
    }
    return hess;
  }

  @Override
  public double[] secOrderGradFull(float[] pred, float label) {
    double[] prob = Maths.floatArrayToDoubleArray(pred);
    Maths.softmax(prob);
    int numLabel = pred.length;
    double[] hess = new double[numLabel * (numLabel + 1) / 2];
    for (int i = 0; i < numLabel; i++) {
      int rowI = Maths.indexOfLowerTriangularMatrix(i, 0);
      for (int j = 0; j < i; j++) {
        hess[rowI + j] = Math.min(-prob[i] * prob[j], -Maths.EPSILON);
      }
      hess[rowI + i] = Math.max(prob[i] * (1.0 - prob[i]), Maths.EPSILON);
    }
    return hess;
  }

  @Override
  public double[] secOrderGradFull(float[] pred, float label, double[] firGrad) {
    int numLabel = pred.length;
    int trueLabel = (int) label;
    double[] prob = new double[numLabel];
    for (int i = 0; i < numLabel; i++) {
      prob[i] = trueLabel == i ? firGrad[i] + 1.0 : firGrad[i];
    }
    double[] hess = new double[numLabel * (numLabel + 1) / 2];
    for (int i = 0; i < numLabel; i++) {
      int rowI = Maths.indexOfLowerTriangularMatrix(i, 0);
      for (int j = 0; j < i; j++) {
        hess[rowI + j] = Math.min(-prob[i] * prob[j], -Maths.EPSILON);
      }
      hess[rowI + i] = Math.max(prob[i] * (1.0 - prob[i]), Maths.EPSILON);
    }
    return hess;
  }

  public static MultinomialLogisticLoss getInstance() {
    if (instance == null) {
      instance = new MultinomialLogisticLoss();
    }
    return instance;
  }
}
