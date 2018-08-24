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


package com.tencent.angel.ml.math2.ufuncs.expression;

public class Tanh extends Unary {
  private double threshold = Math.log(Double.MAX_VALUE);

  public Tanh(boolean inplace) {
    setInplace(inplace);
  }

  @Override public boolean isOrigin() {
    return true;
  }

  @Override public double apply(double elem) {
    if (elem > threshold) {
      return 1;
    } else if (elem < -threshold) {
      return -1;
    } else {
      double val1 = Math.exp(elem);
      double val2 = 1 / val1;
      return (val1 - val2) / (val1 + val2);
    }
  }

  @Override public float apply(float elem) {
    if (elem > threshold) {
      return 1;
    } else if (elem < -threshold) {
      return -1;
    } else {
      double val1 = Math.exp(elem);
      double val2 = 1 / val1;
      return (float) ((val1 - val2) / (val1 + val2));
    }
  }

  @Override public long apply(long elem) {
    if (elem > threshold) {
      return 1;
    } else if (elem < -threshold) {
      return -1;
    } else {
      double val1 = Math.exp(elem);
      double val2 = 1 / val1;
      return (long) ((val1 - val2) / (val1 + val2));
    }
  }

  @Override public int apply(int elem) {
    if (elem > threshold) {
      return 1;
    } else if (elem < -threshold) {
      return -1;
    } else {
      double val1 = Math.exp(elem);
      double val2 = 1 / val1;
      return (int) ((val1 - val2) / (val1 + val2));
    }
  }
}