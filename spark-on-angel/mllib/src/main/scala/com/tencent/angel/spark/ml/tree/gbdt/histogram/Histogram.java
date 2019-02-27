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

import java.io.Serializable;
import java.util.Arrays;


public class Histogram implements Serializable {

  private int numBin;
  private int numClass;
  private boolean fullHessian;
  private double[] gradients;
  private double[] hessians;

  public Histogram(int numBin, int numClass, boolean fullHessian) {
    this.numBin = numBin;
    this.numClass = numClass;
    this.fullHessian = fullHessian;
    if (numClass == 2) {
      this.gradients = new double[numBin];
      this.hessians = new double[numBin];
    } else if (!fullHessian) {
      this.gradients = new double[numBin * numClass];
      this.hessians = new double[numBin * numClass];
    } else {
      this.gradients = new double[numBin * numClass];
      this.hessians = new double[numBin * ((numClass * (numClass + 1)) >> 1)];
    }
  }

  public void accumulate(int index, double grad, double hess) {
    gradients[index] += grad;
    hessians[index] += hess;
  }

  public void accumulate(int index, double[] grad, double[] hess) {
    if (!fullHessian) {
      accumulate(index, grad, hess, 0);
    } else {
      accumulate(index, grad, 0, hess, 0);
    }
  }

  public void accumulate(int index, double[] grad, double[] hess, int offset) {
    int binOffset = index * numClass;
    for (int i = 0; i < numClass; i++) {
      gradients[binOffset + i] += grad[offset + i];
      hessians[binOffset + i] += hess[offset + i];
    }
  }

  public void accumulate(int index, double[] grad, int gradOffset,
      double[] hess, int hessOffset) {
    int gradBinOffset = index * numClass;
    int hessBinOffset = index * ((numClass * (numClass + 1)) >> 1);
    for (int i = 0; i < grad.length; i++) {
      gradients[gradBinOffset + i] += grad[gradOffset + i];
    }
    for (int i = 0; i < hess.length; i++) {
      hessians[hessBinOffset + i] += hess[hessOffset + i];
    }
  }

  public void accumulate(int index, GradPair gradPair) {
    if (numClass == 2) {
      BinaryGradPair binary = (BinaryGradPair) gradPair;
      gradients[index] += binary.getGrad();
      hessians[index] += binary.getHess();
    } else if (!fullHessian) {
      MultiGradPair multi = (MultiGradPair) gradPair;
      double[] grad = multi.getGrad();
      double[] hess = multi.getHess();
      int offset = index * numClass;
      for (int i = 0; i < numClass; i++) {
        gradients[offset + i] += grad[i];
        hessians[offset + i] += hess[i];
      }
    } else {
      MultiGradPair multi = (MultiGradPair) gradPair;
      double[] grad = multi.getGrad();
      double[] hess = multi.getHess();
      int gradOffset = index * numClass;
      int hessOffset = index * ((numClass * (numClass + 1)) >> 1);
      for (int i = 0; i < grad.length; i++) {
        gradients[gradOffset + i] += grad[i];
      }
      for (int i = 0; i < hess.length; i++) {
        hessians[hessOffset + i] += hess[i];
      }
    }
  }

  public Histogram plus(Histogram other) {
    Histogram res = new Histogram(numBin, numClass, fullHessian);
    if (numClass == 2 || !fullHessian) {
      for (int i = 0; i < this.gradients.length; i++) {
        res.gradients[i] = this.gradients[i] + other.gradients[i];
        res.hessians[i] = this.hessians[i] + other.hessians[i];
      }
    } else {
      for (int i = 0; i < this.gradients.length; i++) {
        res.gradients[i] = this.gradients[i] + other.gradients[i];
      }
      for (int i = 0; i < this.hessians.length; i++) {
        res.hessians[i] = this.hessians[i] + other.hessians[i];
      }
    }
    return res;
  }

  public Histogram subtract(Histogram other) {
    Histogram res = new Histogram(numBin, numClass, fullHessian);
    if (numClass == 2 || !fullHessian) {
      for (int i = 0; i < this.gradients.length; i++) {
        res.gradients[i] = this.gradients[i] - other.gradients[i];
        res.hessians[i] = this.hessians[i] - other.hessians[i];
      }
    } else {
      for (int i = 0; i < this.gradients.length; i++) {
        res.gradients[i] = this.gradients[i] - other.gradients[i];
      }
      for (int i = 0; i < this.hessians.length; i++) {
        res.hessians[i] = this.hessians[i] - other.hessians[i];
      }
    }
    return res;
  }

  public void plusBy(Histogram other) {
    if (numClass == 2 || !fullHessian) {
      for (int i = 0; i < this.gradients.length; i++) {
        this.gradients[i] += other.gradients[i];
        this.hessians[i] += other.hessians[i];
      }
    } else {
      for (int i = 0; i < this.gradients.length; i++) {
        this.gradients[i] += other.gradients[i];
      }
      for (int i = 0; i < this.hessians.length; i++) {
        this.hessians[i] += other.hessians[i];
      }
    }
  }

  public void subtractBy(Histogram other) {
    if (numClass == 2 || !fullHessian) {
      for (int i = 0; i < this.gradients.length; i++) {
        this.gradients[i] -= other.gradients[i];
        this.hessians[i] -= other.hessians[i];
      }
    } else {
      for (int i = 0; i < this.gradients.length; i++) {
        this.gradients[i] -= other.gradients[i];
      }
      for (int i = 0; i < this.hessians.length; i++) {
        this.hessians[i] -= other.hessians[i];
      }
    }
  }

  public GradPair sum() {
    return sum(0, numBin);
  }

  public GradPair sum(int start, int end) {
    if (numClass == 2) {
      double sumGrad = 0.0;
      double sumHess = 0.0;
      for (int i = start; i < end; i++) {
        sumGrad += gradients[i];
        sumHess += hessians[i];
      }
      return new BinaryGradPair(sumGrad, sumHess);
    } else if (!fullHessian) {
      double[] sumGrad = new double[numClass];
      double[] sumHess = new double[numClass];
      for (int i = start * numClass; i < end * numClass; i += numClass) {
        for (int j = 0; j < numClass; j++) {
          sumGrad[j] += gradients[i + j];
          sumHess[j] += hessians[i + j];
        }
      }
      return new MultiGradPair(sumGrad, sumHess);
    } else {
      double[] sumGrad = new double[numClass];
      double[] sumHess = new double[(numClass * (numClass + 1)) >> 1];
      for (int i = start; i < end; i++) {
        int gradOffset = i * sumGrad.length;
        for (int j = 0; j < sumGrad.length; j++) {
          sumGrad[j] += gradients[gradOffset + j];
        }
        int hessOffset = i * sumHess.length;
        for (int j = 0; j < sumHess.length; j++) {
          sumHess[j] += hessians[hessOffset + j];
        }
      }
      return new MultiGradPair(sumGrad, sumHess);
    }
  }

  public int getNumBin() {
    return numBin;
  }

  public GradPair get(int index) {
    if (numClass == 2) {
      return new BinaryGradPair(gradients[index], hessians[index]);
    } else {
      double[] grad = Arrays.copyOfRange(gradients,
          index * numClass, (index + 1) * numClass);
      int size = fullHessian ? ((numClass * (numClass + 1)) >> 1) : numClass;
      double[] hess = Arrays.copyOfRange(hessians,
          index * size, (index + 1) * size);
      return new MultiGradPair(grad, hess);
    }
  }

  public void put(int index, GradPair gp) {
    if (numClass == 2) {
      ((BinaryGradPair) gp).set(gradients[index], hessians[index]);
    } else if (!fullHessian) {
      ((MultiGradPair) gp).set(gradients, hessians, index * numClass);
    } else {
      int gradOffset = index * numClass;
      int hessOffset = index * ((numClass * (numClass + 1)) >> 1);
      ((MultiGradPair) gp).set(gradients, gradOffset, hessians, hessOffset);
    }
  }

  public void plusTo(GradPair gp, int index) {
    if (numClass == 2) {
      ((BinaryGradPair) gp).plusBy(gradients[index], hessians[index]);
    } else if (!fullHessian) {
      MultiGradPair multi = (MultiGradPair) gp;
      double[] grad = multi.getGrad();
      double[] hess = multi.getHess();
      int offset = index * numClass;
      for (int i = 0; i < numClass; i++) {
        grad[i] += gradients[offset + i];
        hess[i] += hessians[offset + i];
      }
    } else {
      MultiGradPair multi = (MultiGradPair) gp;
      double[] grad = multi.getGrad();
      double[] hess = multi.getHess();
      int gradOffset = index * grad.length;
      int hessOffset = index * hess.length;
      for (int i = 0; i < grad.length; i++) {
        grad[i] += gradients[gradOffset + i];
      }
      for (int i = 0; i < hess.length; i++) {
        hess[i] += hessians[hessOffset + i];
      }
    }
  }

  public void subtractTo(GradPair gp, int index) {
    if (numClass == 2) {
      ((BinaryGradPair) gp).subtractBy(gradients[index], hessians[index]);
    } else if (!fullHessian) {
      MultiGradPair multi = (MultiGradPair) gp;
      double[] grad = multi.getGrad();
      double[] hess = multi.getHess();
      int offset = index * numClass;
      for (int i = 0; i < numClass; i++) {
        grad[i] -= gradients[offset + i];
        hess[i] -= hessians[offset + i];
      }
    } else {
      MultiGradPair multi = (MultiGradPair) gp;
      double[] grad = multi.getGrad();
      double[] hess = multi.getHess();
      int gradOffset = index * grad.length;
      int hessOffset = index * hess.length;
      for (int i = 0; i < grad.length; i++) {
        grad[i] -= gradients[gradOffset + i];
      }
      for (int i = 0; i < hess.length; i++) {
        hess[i] -= hessians[hessOffset + i];
      }
    }
  }

  public void scan(int index, GradPair left, GradPair right) {
    if (numClass == 2) {
      ((BinaryGradPair) left).plusBy(gradients[index], hessians[index]);
      ((BinaryGradPair) right).subtractBy(gradients[index], hessians[index]);
    } else if (!fullHessian) {
      MultiGradPair leftMulti = (MultiGradPair) left;
      double[] leftGrad = leftMulti.getGrad();
      double[] leftHess = leftMulti.getHess();
      MultiGradPair rightMulti = (MultiGradPair) right;
      double[] rightGrad = rightMulti.getGrad();
      double[] rightHess = rightMulti.getHess();
      int offset = index * numClass;
      for (int i = 0; i < numClass; i++) {
        leftGrad[i] += gradients[offset + i];
        leftHess[i] += hessians[offset + i];
        rightGrad[i] -= gradients[offset + i];
        rightHess[i] -= hessians[offset + i];
      }
    } else {
      MultiGradPair leftMulti = (MultiGradPair) left;
      double[] leftGrad = leftMulti.getGrad();
      double[] leftHess = leftMulti.getHess();
      MultiGradPair rightMulti = (MultiGradPair) right;
      double[] rightGrad = rightMulti.getGrad();
      double[] rightHess = rightMulti.getHess();
      int gradOffset = index * leftGrad.length;
      int hessOffset = index * leftHess.length;
      for (int i = 0; i < leftGrad.length; i++) {
        leftGrad[i] += gradients[gradOffset + i];
        rightGrad[i] -= gradients[gradOffset + i];
      }
      for (int i = 0; i < leftHess.length; i++) {
        leftHess[i] += hessians[hessOffset + i];
        rightHess[i] -= hessians[hessOffset + i];
      }
    }
  }

  public void clear() {
    Arrays.fill(gradients, 0.0);
    Arrays.fill(hessians, 0.0);
  }

}
