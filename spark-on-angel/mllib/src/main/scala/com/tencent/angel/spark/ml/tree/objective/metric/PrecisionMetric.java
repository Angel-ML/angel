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
import com.tencent.angel.spark.ml.tree.util.Maths;


@Singleton
public class PrecisionMetric implements EvalMetric {

  private static PrecisionMetric instance;

  private PrecisionMetric() {
  }

  @Override
  public Kind getKind() {
    return Kind.PRECISION;
  }

  @Override
  public double sum(float[] preds, float[] labels) {
    return sum(preds, labels, 0, labels.length);
  }

  @Override
  public double sum(float[] preds, float[] labels, int start, int end) {
    double correct = 0.0;
    if (preds.length == labels.length) {
      for (int i = start; i < end; i++) {
        correct += evalOne(preds[i], labels[i]);
      }
    } else {
      int numLabel = preds.length / labels.length;
      float[] pred = new float[numLabel];
      for (int i = start; i < end; i++) {
        System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
        correct += evalOne(pred, labels[i]);
      }
    }
    return correct;
  }

  @Override
  public double avg(double sum, int num) {
    return sum / num;
  }

  @Override
  public double eval(float[] preds, float[] labels) {
    return avg(sum(preds, labels), labels.length);
//        double correct = 0.0;
//        if (preds.length == labels.length) {
//            for (int i = 0; i < preds.length; i++) {
//                correct += evalOne(preds[i], labels[i]);
//            }
//        } else {
//            int numLabel = preds.length / labels.length;
//            float[] pred = new float[numLabel];
//            for (int i = 0; i < labels.length; i++) {
//                System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
//                correct += evalOne(pred, labels[i]);
//            }
//        }
//        return (float) (correct / labels.length);
  }

  @Override
  public double evalOne(float pred, float label) {
    return pred < 0.0f ? 1.0 - label : label;
  }

  @Override
  public double evalOne(float[] pred, float label) {
    return Maths.argmax(pred) == ((int) label) ? 1 : 0;
  }

  public static PrecisionMetric getInstance() {
    if (instance == null) {
      instance = new PrecisionMetric();
    }
    return instance;
  }
}
