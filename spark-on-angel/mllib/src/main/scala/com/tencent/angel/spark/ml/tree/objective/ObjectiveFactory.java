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

package com.tencent.angel.spark.ml.tree.objective;

import com.tencent.angel.spark.ml.tree.objective.loss.Loss;
import com.tencent.angel.spark.ml.tree.exception.GBDTException;
import com.tencent.angel.spark.ml.tree.objective.loss.*;
import com.tencent.angel.spark.ml.tree.objective.metric.*;

public class ObjectiveFactory {

  public static Loss getLoss(Loss.Kind lossFunc) {
    switch (lossFunc) {
      case RMSE:
        return RMSELoss.getInstance();
      case BinaryLogistic:
        return BinaryLogisticLoss.getInstance();
      case MultiLogistic:
        return MultinomialLogisticLoss.getInstance();
      default:
        throw new GBDTException("Unrecognizable loss function: " + lossFunc);
    }
  }

  public static BinaryLoss getBinaryLoss(Loss.Kind lossFunc) {
    switch (lossFunc) {
      case RMSE:
        return RMSELoss.getInstance();
      case BinaryLogistic:
        return BinaryLogisticLoss.getInstance();
      case MultiLogistic:
        throw new GBDTException("Loss function " + lossFunc
            + " is not a binary loss function");
      default:
        throw new GBDTException("Unrecognizable loss function: " + lossFunc);
    }
  }

  public static MultiLoss getMultiLoss(Loss.Kind lossFunc) {
    switch (lossFunc) {
      case RMSE:
        return RMSELoss.getInstance();
      case MultiLogistic:
        return MultinomialLogisticLoss.getInstance();
      case BinaryLogistic:
        throw new GBDTException("Loss function " + lossFunc
            + " is not a multi-class loss function");
      default:
        throw new GBDTException("Unrecognizable loss function: " + lossFunc);
    }
  }

  public static Loss getLoss(String lossFunc) {
    for (Loss.Kind kind : Loss.Kind.values()) {
      if (lossFunc.equalsIgnoreCase(kind.toString())) {
        return getLoss(kind);
      }
    }
    throw new GBDTException("Unrecognizable loss function: " + lossFunc);
  }

  public static BinaryLoss getBinaryLoss(String lossFunc) {
    for (Loss.Kind kind : Loss.Kind.values()) {
      if (lossFunc.equalsIgnoreCase(kind.toString())) {
        return getBinaryLoss(kind);
      }
    }
    throw new GBDTException("Unrecognizable loss function: " + lossFunc);
  }

  public static MultiLoss getMultiLoss(String lossFunc) {
    for (Loss.Kind kind : Loss.Kind.values()) {
      if (lossFunc.equalsIgnoreCase(kind.toString())) {
        return getMultiLoss(kind);
      }
    }
    throw new GBDTException("Unrecognizable loss function: " + lossFunc);
  }

  public static EvalMetric getEvalMetric(EvalMetric.Kind metric) {
    switch (metric) {
      case RMSE:
        return RMSEMetric.getInstance();
      case ERROR:
        return ErrorMetric.getInstance();
      case PRECISION:
        return PrecisionMetric.getInstance();
      case LOG_LOSS:
        return LogLossMetric.getInstance();
      case CROSS_ENTROPY:
        return CrossEntropyMetric.getInstance();
      case AUC:
        return new AUCMetric();
      default:
        throw new GBDTException("Unrecognizable eval metric: " + metric);
    }
  }

  public static EvalMetric getEvalMetric(String metric) {
    for (EvalMetric.Kind kind : EvalMetric.Kind.values()) {
      if (metric.equalsIgnoreCase(kind.toString())) {
        return getEvalMetric(kind);
      }
    }
    throw new GBDTException("Unrecognizable eval metric: " + metric);
  }

  public static EvalMetric getEvalMetricOrDefault(String metric, Loss loss) {
    if (metric == null || metric.length() == 0) {
      return getEvalMetric(loss.defaultEvalMetric());
    } else {
      return getEvalMetric(metric);
    }
  }

  public static EvalMetric[] getEvalMetrics(String[] metrics) {
    if (metrics.length == 0) {
      throw new GBDTException("No eval metric specified");
    }

    EvalMetric[] result = new EvalMetric[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      result[i] = getEvalMetric(metrics[i]);
    }
    return result;
  }

  public static EvalMetric[] getEvalMetricsOrDefault(String[] metrics, Loss loss) {
    if (metrics == null || metrics.length == 0) {
      return new EvalMetric[]{getEvalMetric(loss.defaultEvalMetric())};
    } else {
      return getEvalMetrics(metrics);
    }
  }

  public static MultiStrategy getMultiStrategy(String name) {
    for (MultiStrategy strategy : MultiStrategy.values()) {
      if (name.equalsIgnoreCase(strategy.toString())) {
        return strategy;
      }
    }
    throw new GBDTException("Unrecognizable multi strategy: " + name);
  }

}
