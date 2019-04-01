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

public class Dropout extends Unary {

  private double proba;

  public Dropout(boolean inplace, double proba) {
    assert (proba >= 0 && proba <= 1);
    this.proba = proba;
    setInplace(inplace);
  }

  @Override
  public boolean isOrigin() {
    return true;
  }

  @Override
  public double apply(double elem) {
    if (proba == 1) {
      return 0;
    } else if (Math.random() < 1 - proba) {
      return elem / (1 - proba);
    } else {
      return 0;
    }
  }

  @Override
  public float apply(float elem) {
    if (proba == 1) {
      return 0;
    } else if (Math.random() < 1 - proba) {
      return (float) (elem / (1 - proba));
    } else {
      return 0;
    }
  }

  @Override
  public long apply(long elem) {
    if (proba == 1) {
      return 0;
    } else if (Math.random() < 1 - proba) {
      return (long) (elem / (1 - proba));
    } else {
      return 0;
    }
  }

  @Override
  public int apply(int elem) {
    if (proba == 1) {
      return 0;
    } else if (Math.random() < 1 - proba) {
      return (int) (elem / (1 - proba));
    } else {
      return 0;
    }
  }
}