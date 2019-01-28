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

package com.tencent.angel.spark.ml.tree.tree.param;

import java.util.Arrays;
import com.tencent.angel.spark.ml.tree.util.Maths;

public class GBDTParam extends RegTParam {

  public int numClass; // number of classes/labels
  public int numTree;  // number of trees
  public int numThread;  // parallelism

  public boolean histSubtraction;
  public boolean lighterChildFirst;
  //public boolean leafwise;  // true if leaf-wise training, false if level-wise training

  public boolean fullHessian;  // whether to use full hessian matrix instead of diagonal
  public float minChildWeight;  // minimum amount of hessian (weight) allowed for a child
  public int minNodeInstance;
  public float regAlpha;  // L1 regularization factor
  public float regLambda;  // L2 regularization factor
  public float maxLeafWeight; // maximum leaf weight, default 0 means no constraints

  public String lossFunc; // name of loss function
  public String[] evalMetrics; // name of eval metric

  /**
   * Whether the sum of hessian satisfies weight
   *
   * @param sumHess sum of hessian values
   * @return true if satisfied, false otherwise
   */
  public boolean satisfyWeight(double sumHess) {
    return sumHess >= minChildWeight;
  }

  public boolean satisfyWeight(double sumGrad, double sumHess) {
    return sumGrad != 0.0f && satisfyWeight(sumHess);
  }

  /**
   * Whether the sum of hessian satisfies weight Since hessian matrix is positive, we have det(hess)
   * <= a11*a22*...*akk, thus we approximate det(hess) with a11*a22*...*akk
   *
   * @param sumHess sum of hessian values
   * @return true if satisfied, false otherwise
   */
  public boolean satisfyWeight(double[] sumHess) {
    if (minChildWeight == 0.0f) {
      return true;
    }
    double w = 1.0;
    if (!fullHessian) {
      for (double h : sumHess) {
        w *= h;
      }
    } else {
      for (int k = 0; k < numClass; k++) {
        int index = Maths.indexOfLowerTriangularMatrix(k, k);
        w *= sumHess[index];
      }
    }
    return w >= minChildWeight;
  }

  public boolean satisfyWeight(double[] sumGrad, double[] sumHess) {
    return !Maths.areZeros(sumGrad) && satisfyWeight(sumHess);
  }

  /**
   * Calculate leaf weight given the statistics
   *
   * @param sumGrad sum of gradient values
   * @param sumHess sum of hessian values
   * @return weight
   */
  public double calcWeight(double sumGrad, double sumHess) {
    if (!satisfyWeight(sumHess) || sumGrad == 0.0) {
      return 0.0;
    }
    double dw;
    if (regAlpha == 0.0f) {
      dw = -sumGrad / (sumHess + regLambda);
    } else {
      dw = -Maths.thresholdL1(sumGrad, regAlpha) / (sumHess + regLambda);
    }
    if (maxLeafWeight != 0.0f) {
      if (dw > maxLeafWeight) {
        dw = maxLeafWeight;
      } else if (dw < -maxLeafWeight) {
        dw = -maxLeafWeight;
      }
    }
    return dw;
  }

  public double[] calcWeights(double[] sumGrad, double[] sumHess) {
    double[] weights = new double[numClass];
    if (!satisfyWeight(sumHess) || Maths.areZeros(sumGrad)) {
      return weights;
    }
    // TODO: regularization
    if (!fullHessian) {
      if (regAlpha == 0.0f) {
        for (int k = 0; k < numClass; k++) {
          weights[k] = -sumGrad[k] / (sumHess[k] + regLambda);
        }
      } else {
        for (int k = 0; k < numClass; k++) {
          weights[k] = -Maths.thresholdL1(sumGrad[k], regAlpha) / (sumHess[k] + regLambda);
        }
      }
    } else {
      addDiagonal(numClass, sumHess, regLambda);
      weights = Maths.solveLinearSystemWithCholeskyDecomposition(sumHess, sumGrad, numClass);
      for (int i = 0; i < numClass; i++) {
        weights[i] *= -1;
      }
      addDiagonal(numClass, sumHess, -regLambda);
    }
    if (maxLeafWeight != 0.0f) {
      for (int k = 0; k < numClass; k++) {
        if (weights[k] > maxLeafWeight) {
          weights[k] = maxLeafWeight;
        } else if (weights[k] < -maxLeafWeight) {
          weights[k] = -maxLeafWeight;
        }
      }
    }
    return weights;
  }

  /**
   * Calculate the cost of loss function
   *
   * @param sumGrad sum of gradient values
   * @param sumHess sum of hessian values
   * @return loss gain
   */
  public double calcGain(double sumGrad, double sumHess) {
    if (!satisfyWeight(sumHess) || sumGrad == 0.0f) {
      return 0.0f;
    }
    if (maxLeafWeight == 0.0f) {
      if (regAlpha == 0.0f) {
        return (sumGrad / (sumHess + regLambda)) * sumGrad;
      } else {
        return Maths.sqr(Maths.thresholdL1(sumGrad, regAlpha)) / (sumHess + regLambda);
      }
    } else {
      double w = calcWeight(sumGrad, sumHess);
      double ret = sumGrad * w + 0.5 * (sumHess + regLambda) * Maths.sqr(w);
      if (regAlpha == 0.0f) {
        return -2.0 * ret;
      } else {
        return -2.0 * (ret + regAlpha * Math.abs(w));
      }
    }
  }

  public double calcGain(double[] sumGrad, double[] sumHess) {
    double gain = 0.0;
    if (!satisfyWeight(sumHess) || Maths.areZeros(sumGrad)) {
      return 0.0;
    }
    // TODO: regularization
    if (!fullHessian) {
      if (regAlpha == 0.0f) {
        for (int k = 0; k < numClass; k++) {
          gain += sumGrad[k] / (sumHess[k] + regLambda) * sumGrad[k];
        }
      } else {
        for (int k = 0; k < numClass; k++) {
          gain += Maths.sqr(Maths.thresholdL1(sumGrad[k], regAlpha)) * (sumHess[k] + regLambda);
        }
      }
    } else {
      addDiagonal(numClass, sumHess, regLambda);
      double[] tmp = Maths.solveLinearSystemWithCholeskyDecomposition(sumHess, sumGrad, numClass);
      gain = Maths.dot(sumGrad, tmp);
      addDiagonal(numClass, sumHess, -regLambda);
    }
    return (float) (gain / numClass);
  }

  private void addDiagonal(int n, double[] sumHess, double v) {
    for (int i = 0; i < n; i++) {
      int index = Maths.indexOfLowerTriangularMatrix(i, i);
      sumHess[index] += v;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(String.format("|numClass = %d\n", numClass));
    sb.append(String.format("|numTree = %d\n", numTree));
    sb.append(String.format("|numThread = %d\n", numThread));
    sb.append(String.format("|fullHessian = %s\n", fullHessian));
    sb.append(String.format("|minChildWeight = %f\n", minChildWeight));
    sb.append(String.format("|minNodeInstance = %d\n", minNodeInstance));
    sb.append(String.format("|regAlpha = %s\n", regAlpha));
    sb.append(String.format("|regLambda = %s\n", regLambda));
    sb.append(String.format("|maxLeafWeight = %s\n", maxLeafWeight));
    sb.append(String.format("|lossFunc = %s\n", lossFunc));
    sb.append(String.format("|evalMetrics = %s\n", Arrays.toString(evalMetrics)));
    return sb.toString();
  }
}

