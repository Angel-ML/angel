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


package com.tencent.angel.spark.ml.tree.gbdt.histogram;

import com.tencent.angel.spark.ml.tree.param.GBDTParam;
import com.tencent.angel.spark.ml.tree.util.Maths;
import java.io.Serializable;
import java.util.Arrays;


public class MultiGradPair implements GradPair, Serializable {

  private double[] grad;
  private double[] hess;

  public MultiGradPair(int numClass, boolean fullHessian) {
    this.grad = new double[numClass];
    if (fullHessian) {
      this.hess = new double[(numClass * (numClass + 1)) >> 1];
    } else {
      this.hess = new double[numClass];
    }
  }

  public MultiGradPair(double[] grad, double[] hess) {
    this.grad = grad;
    this.hess = hess;
  }

  @Override
  public void plusBy(GradPair gradPair) {
    double[] grad = ((MultiGradPair) gradPair).grad;
    double[] hess = ((MultiGradPair) gradPair).hess;
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] += grad[i];
    }
    for (int i = 0; i < this.hess.length; i++) {
      this.hess[i] += hess[i];
    }
  }

  public void plusBy(double[] grad, double[] hess) {
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] += grad[i];
    }
    for (int i = 0; i < this.hess.length; i++) {
      this.hess[i] += hess[i];
    }
  }

  @Override
  public void subtractBy(GradPair gradPair) {
    double[] grad = ((MultiGradPair) gradPair).grad;
    double[] hess = ((MultiGradPair) gradPair).hess;
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] -= grad[i];
    }
    for (int i = 0; i < this.hess.length; i++) {
      this.hess[i] -= hess[i];
    }
  }

  public void subtractBy(double[] grad, double[] hess) {
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] -= grad[i];
    }
    for (int i = 0; i < this.hess.length; i++) {
      this.hess[i] -= hess[i];
    }
  }

  @Override
  public GradPair plus(GradPair gradPair) {
    GradPair res = this.copy();
    res.plusBy(gradPair);
    return res;
  }

  public GradPair plus(double[] grad, double[] hess) {
    MultiGradPair res = this.copy();
    res.plusBy(grad, hess);
    return res;
  }

  @Override
  public GradPair subtract(GradPair gradPair) {
    GradPair res = this.copy();
    res.subtractBy(gradPair);
    return res;
  }

  public GradPair subtract(double[] grad, double[] hess) {
    MultiGradPair res = this.copy();
    res.plusBy(grad, hess);
    return res;
  }

  @Override
  public void timesBy(double x) {
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] *= x;
    }
    for (int i = 0; i < this.hess.length; i++) {
      this.hess[i] *= x;
    }
  }

  @Override
  public float calcGain(GBDTParam param) {
    return (float) param.calcGain(grad, hess);
  }

  public float[] calcWeights(GBDTParam param) {
    //return param.calcWeights(grad, hess);
    return Maths.doubleArrayToFloatArray(param.calcWeights(grad, hess));
  }

  @Override
  public boolean satisfyWeight(GBDTParam param) {
    return param.satisfyWeight(hess);
  }

  @Override
  public MultiGradPair copy() {
    return new MultiGradPair(grad.clone(), hess.clone());
  }

  public double[] getGrad() {
    return grad;
  }

  public double[] getHess() {
    return hess;
  }

  public void setGrad(double[] grad) {
    this.grad = grad;
  }

  public void setHess(double[] hess) {
    this.hess = hess;
  }

  public void set(double[] grad, double[] hess) {
    this.grad = grad;
    this.hess = hess;
  }

  public void set(double[] grad, double[] hess, int offset) {
    // numClass is usually small, so we do not use arraycopy here
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] = grad[i + offset];
      this.hess[i] = hess[i + offset];
    }
  }

  public void set(double[] grad, int gradOffset, double[] hess, int hessOffset) {
    // numClass is usually small, so we do not use arraycopy here
    for (int i = 0; i < this.grad.length; i++) {
      this.grad[i] = grad[i + gradOffset];
    }
    for (int i = 0; i < this.hess.length; i++) {
      this.hess[i] = hess[i + hessOffset];
    }
  }

  @Override
  public String toString() {
    String gradStr = Arrays.toString(grad);
    if (grad.length == hess.length) {
      return "(" + gradStr + ", diag{" + Arrays.toString(hess) + "})";
    } else {
      int rowSize = 1, offset = 0;
      StringBuilder hessSB = new StringBuilder("[");
      while (rowSize <= grad.length) {
        hessSB.append("[");
        hessSB.append(hess[offset]);
        for (int i = 1; i < rowSize; i++) {
          hessSB.append(", ");
          hessSB.append(hess[offset + i]);
        }
        hessSB.append("]");
        offset += rowSize;
        rowSize++;
      }
      hessSB.append("]");
      return "(" + gradStr + ", " + hessSB.toString() + ")";
    }
  }
}
