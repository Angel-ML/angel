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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The type L 2 hinge loss.
 */
public class L2HingeLoss extends L2Loss {
  private static final Log LOG = LogFactory.getLog(L2HingeLoss.class);

  /**
   * Instantiates a new L 2 hinge loss.
   *
   * @param lamda: the regularization coefficient
   */
  public L2HingeLoss(double lamda) {
    this.lambda = lamda;
  }

  /**
   * calculate SVM's loss of a sample, loss = max{0, 1-y*w*x}
   *
   * @param x : feature vector
   * @param y : ground truth
   * @param w : weight vector
   * @return the double
   */
  public double loss(TAbstractVector x, double y, TVector w) {
    double pre = w.dot(x);
    return loss(pre, y);
  }

  /**
   * calculate SVM's loss of a sample, loss = max{0, 1-y*pre}
   *
   * @param pre: predictive value
   * @param y:   ground truth
   */
  @Override public double loss(double pre, double y) {
    double z = pre * y;
    if (z < 1) {
      return 1 - z;
    }
    return 0.0;

    //    double z=pre*y;
    //    if(z<=0) return 0.5-z;
    //    else if(z>0 && z<1) return 0.5*Math.pow(1-z,2);
    //    return 0.0;
  }

  /**
   * calculate SVM's loss of a batch, loss = 0.5*regParam*w*w + sum(max{0, 1-y*w*x})
   *
   * @param xList     : training samples
   * @param yList     : training labels
   * @param w         : weight vector
   * @param batchSize : number of training samples
   * @return the double
   */
  public double loss(TDoubleVector[] xList, double[] yList, TDoubleVector w, int batchSize) {
    double loss = 0.0;
    for (int i = 0; i < batchSize; i++) {
      loss += loss(xList[i], yList[i], w);
    }
    loss += getReg(w);
    return loss;
  }

  @Override public double grad(double pre, double y) {
    if (pre * y <= 1) {
      return y;
    } else {
      return 0.0;
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
