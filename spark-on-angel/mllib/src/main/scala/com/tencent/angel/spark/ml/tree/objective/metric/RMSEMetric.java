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

import javax.inject.Singleton;


@Singleton
public class RMSEMetric implements EvalMetric {

  private static RMSEMetric instance;

  private RMSEMetric() {
  }

  @Override
  public Kind getKind() {
    return Kind.RMSE;
  }

  @Override
  public double sum(float[] preds, float[] labels) {
    return sum(preds, labels, 0, labels.length);
  }

  @Override
  public double sum(float[] preds, float[] labels, int start, int end) {
    double errSum = 0.0f;
    if (preds.length == labels.length) {
      for (int i = start; i < end; i++) {
        errSum += evalOne(preds[i], labels[i]);
      }
    } else {
      int numLabel = preds.length / labels.length;
      float[] pred = new float[numLabel];
      for (int i = start; i < end; i++) {
        System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
        errSum += evalOne(pred, labels[i]);
      }
    }
    return errSum;
  }

  @Override
  public double avg(double sum, int num) {
    return Math.sqrt(sum / num);
  }

  @Override
  public double eval(float[] preds, float[] labels) {
    return avg(sum(preds, labels), labels.length);
//        double errSum = 0.0f;
//        if (preds.length == labels.length) {
//            for (int i = 0; i < preds.length; i++) {
//                errSum += evalOne(preds[i], labels[i]);
//            }
//        } else {
//            int numLabel = preds.length / labels.length;
//            float[] pred = new float[numLabel];
//            for (int i = 0; i < labels.length; i++) {
//                System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
//                errSum += evalOne(pred, labels[i]);
//            }
//        }
//        return Math.sqrt(errSum / labels.length);
  }

  @Override
  public double evalOne(float pred, float label) {
    double diff = pred - label;
    return diff * diff;
  }

  @Override
  public double evalOne(float[] pred, float label) {
    double err = 0.0;
    int trueLabel = (int) label;
    for (int i = 0; i < pred.length; i++) {
      double diff = pred[i] - (i == trueLabel ? 1 : 0);
      err += diff * diff;
    }
    return err;
  }

  public static RMSEMetric getInstance() {
    if (instance == null) {
      instance = new RMSEMetric();
    }
    return instance;
  }
}
