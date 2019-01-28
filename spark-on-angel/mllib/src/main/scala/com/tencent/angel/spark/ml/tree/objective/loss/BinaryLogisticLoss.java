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
public class BinaryLogisticLoss implements BinaryLoss {

  private static BinaryLogisticLoss instance;

  private BinaryLogisticLoss() {
  }

  @Override
  public Kind getKind() {
    return Kind.BinaryLogistic;
  }

  @Override
  public EvalMetric.Kind defaultEvalMetric() {
    return EvalMetric.Kind.LOG_LOSS;
  }

  @Override
  public double firOrderGrad(float pred, float label) {
    double prob = Maths.fastSigmoid(pred);
    return prob - label;
  }

  @Override
  public double secOrderGrad(float pred, float label) {
    double prob = Maths.fastSigmoid(pred);
    return Math.max(prob * (1 - prob), Maths.EPSILON);
  }

  @Override
  public double secOrderGrad(float pred, float label, double firGrad) {
    double prob = firGrad + label;
    return Math.max(prob * (1 - prob), Maths.EPSILON);
  }

  public static BinaryLogisticLoss getInstance() {
    if (instance == null) {
      instance = new BinaryLogisticLoss();
    }
    return instance;
  }
}
