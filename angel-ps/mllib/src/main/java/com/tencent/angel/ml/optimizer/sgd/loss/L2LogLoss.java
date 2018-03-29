/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.optimizer.sgd.loss;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;

/**
 * The type L 2 log loss.
 */
public class L2LogLoss extends L2Loss {

  /**
   * Instantiates a new l2-logloss.
   *
   * @param lamda: the regularization coefficient
   */
  public L2LogLoss(double lamda) {
    this.lambda = lamda;
  }

  /**
   * calculate logloss(x,y,w) = log(1+exp(-w*x*y))
   *
   * @param x : training sample
   * @param y : ground truth
   * @param w : weight vector
   * @return double
   */
  public double loss(TAbstractVector x, double y, TVector w) {
    double pre = w.dot(x);
    return loss(pre, y);
  }

  /**
   * calculate logloss(a,y) = log(1+exp(-a*y)), in which a = w.dot(x)
   *
   * @param pre: predictive value
   * @param y:   ground truth
   * @return
   */
  @Override public double loss(double pre, double y) {
    double z = pre * y;
    if (z > 18) {
      return Math.exp(-z);
    } else if (z < -18) {
      return -z;
    }
    return Math.log(1 + Math.exp(-z));
  }

  /**
   * calculte regularized logloss of a training batch, logloss(w,x,y) = 0.5*lambda*w*w +
   * log(1+exp(-w*x*y))
   *
   * @param xList     : training samples
   * @param yList     : training labels
   * @param w         : weight vector
   * @param batchSize : number of training samples
   * @return double double
   */
  public double loss(TAbstractVector[] xList, double[] yList, TDoubleVector w, int batchSize) {
    double loss = 0.0;
    for (int i = 0; i < batchSize; i++) {
      loss += loss(xList[i], yList[i], w);
    }
    loss += getReg(w);
    return loss;
  }

  /**
   * calculate gradient of log loss function d(log(1+exp(-pre*y)))/d(pre) =
   * y*exp(-pre*y)/(1+exp(-pre*y), we omit the negative sign here
   *
   * @param pre: predictive value
   * @param y:   ground truth
   * @return
   */
  @Override public double grad(double pre, double y) {
    double z = pre * y;
    if (z > 18) {
      return y * Math.exp(-z);
    } else if (z < -18) {
      return y;
    } else {
      return y / (1.0 + Math.exp(z));
    }
  }

  /**
   * predict the label of a sample
   *
   * @param w: weight vector
   * @param x: feature vector of a sample
   */
  @Override public double predict(TDoubleVector w, TVector x) {
    return w.dot(x);
  }

}
