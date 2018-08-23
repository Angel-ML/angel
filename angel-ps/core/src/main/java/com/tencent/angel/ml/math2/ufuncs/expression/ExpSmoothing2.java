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

import com.tencent.angel.ml.math2.utils.Constant;


public class ExpSmoothing2 extends Binary {
  private double factor;

  public ExpSmoothing2(boolean inplace, double factor) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.factor = factor;
  }

  @Override public OpType getOpType() {
    return OpType.UNION;
  }

  @Override public double apply(double ele1, double ele2) {
    return factor * ele1 + (1 - factor) * ele2 * ele2;
  }

  @Override public double apply(double ele1, float ele2) {
    return factor * ele1 + (1 - factor) * ele2 * ele2;
  }

  @Override public double apply(double ele1, long ele2) {
    return factor * ele1 + (1 - factor) * ele2 * ele2;
  }

  @Override public double apply(double ele1, int ele2) {
    return factor * ele1 + (1 - factor) * ele2 * ele2;
  }

  @Override public float apply(float ele1, float ele2) {
    return (float) (factor * ele1 + (1 - factor) * ele2 * ele2);
  }

  @Override public float apply(float ele1, long ele2) {
    return (float) (factor * ele1 + (1 - factor) * ele2 * ele2);
  }

  @Override public float apply(float ele1, int ele2) {
    return (float) (factor * ele1 + (1 - factor) * ele2 * ele2);
  }

  @Override public long apply(long ele1, long ele2) {
    return (long) (factor * ele1 + (1 - factor) * ele2 * ele2);
  }

  @Override public long apply(long ele1, int ele2) {
    return (long) (factor * ele1 + (1 - factor) * ele2 * ele2);
  }

  @Override public int apply(int ele1, int ele2) {
    return (int) (factor * ele1 + (1 - factor) * ele2 * ele2);
  }
}
