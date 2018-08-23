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

public class Replace extends Unary {
  double x;
  double val;

  public Replace(boolean inplace, double x, double val) {
    setInplace(inplace);
    this.x = x;
    this.val = val;
  }

  @Override public double apply(double elem) {
    if (elem > x) {
      return 1;
    } else {
      return val;
    }
  }

  @Override public float apply(float elem) {
    if (elem > x) {
      return 1;
    } else {
      return (float) val;
    }
  }

  @Override public long apply(long elem) {
    if (elem > x) {
      return 1;
    } else {
      return (long) val;
    }
  }

  @Override public int apply(int elem) {
    if (elem > x) {
      return 1;
    } else {
      return (int) val;
    }
  }
}
