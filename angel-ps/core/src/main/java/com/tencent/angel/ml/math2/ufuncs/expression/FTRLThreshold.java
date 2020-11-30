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

public class FTRLThreshold extends Binary {

  private double alpha, beta, lambda1, lambda2;

  public FTRLThreshold(boolean inplace, double alpha, double beta, double lambda1, double lambda2) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.alpha = alpha;
    this.beta = beta;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
  }

  @Override
  public OpType getOpType() {
    return OpType.UNION;
  }

  @Override
  public double apply(double zVal, double nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  @Override
  public double apply(double zVal, float nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  public double apply(double zVal, long nVal) {

    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  public double apply(double zVal, int nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  public float apply(float zVal, float nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return (float) result;
  }

  public float apply(float zVal, long nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return (float) result;
  }

  public float apply(float zVal, int nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return (float) result;
  }

  public long apply(long zVal, long nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return (long) result;
  }

  public long apply(long zVal, int nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return (long) result;
  }

  public int apply(int zVal, int nVal) {
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
          - Math.signum(zVal) * lambda1);
    }
    return (int) result;
  }
}