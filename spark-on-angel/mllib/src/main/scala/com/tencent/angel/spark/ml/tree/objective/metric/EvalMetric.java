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

package com.tencent.angel.spark.ml.tree.objective.metric;

public interface EvalMetric {

  Kind getKind();

  double sum(float[] preds, float[] labels);

  double sum(float[] preds, float[] labels, int start, int end);

  double avg(double sum, int num);

  double eval(float[] preds, float[] labels);

  double evalOne(float pred, float label);

  double evalOne(float[] pred, float label);

  public enum Kind {
    RMSE("rmse"),
    ERROR("error"),
    LOG_LOSS("log-loss"),
    CROSS_ENTROPY("cross-entropy"),
    PRECISION("precision"),
    AUC("auc");

    private final String kind;

    private Kind(String kind) {
      this.kind = kind;
    }

    @Override
    public String toString() {
      return kind;
    }
  }
}
