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
package com.tencent.angel.ml.param;

import com.tencent.angel.ml.utils.MathUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Description: hyper-parameter of regression tree model
 */

public class RegTParam implements TrainParam{
  private static final Log LOG = LogFactory.getLog(RegTParam.class);

  // step size for a tree
  public float learningRate = 0.1f;
  // number of class
  public int numClass = 2;
  // minimum loss change required for a split
  public float minSplitLoss = 0;
  // maximum depth of a tree
  public int maxDepth = 6;
  // number of features
  public int numFeature;
  // number of nonzero
  public int numNonzero;
  // number of candidates split value
  public int numSplit = 10;
  // ----- the rest parameters are less important ----
  // base instance weight
  public float baseWeight = 0;
  // minimum amount of hessian(weight) allowed in a child
  public float minChildWeight = 0.01f;
  // L2 regularization factor
  public float regLambda = 1.0f;
  // L1 regularization factor
  public float regAlpha = 0;
  // default direction choice
  public int defaultDirection;
  // maximum delta update we can add in weight estimation
  // this parameter can be used to stabilize update
  // default=0 means no constraint on weight delta
  public float maxDeltaStep = 0.0f;
  // whether we want to do subsample for row
  public float rowSample = 1.0f;
  // whether to subsample columns for each tree
  public float colSample = 1.0f;
  // accuracy of sketch
  public float sketchEps = 0.03f;
  // accuracy of sketch
  public float sketchRatio = 2.0f;
  // leaf vector size
  public int sizeLeafVector = 0;
  // option for parallelization
  public int parallelOption = 0;
  // option to open cacheline optimization
  public boolean cacheOpt = true;
  // whether to not print info during training.
  public boolean silent = false;

  @Override
  public void printParam() {
    LOG.info(String.format("Tree hyper-parameters------"
        + "maxdepth: %d, minSplitLoss: %f, rowSample: %f, colSample: %f", this.maxDepth,
        this.minSplitLoss, this.rowSample, this.colSample));
  }

  /**
   * calculate weight given the statistics.
   *
   * @param sumGrad the sum grad
   * @param sumHess the sum hess
   * @return the float
   */
  public float calcWeight(float sumGrad, float sumHess) {
    if (sumHess < minChildWeight) {
      return 0.0f;
    }
    float dw;
    if (regAlpha == 0.0f) {
      dw = -sumGrad / (sumHess + regLambda);
    } else {
      dw = -MathUtils.thresholdL1(sumGrad, regAlpha) / (sumHess + regLambda);
    }
    if (maxDeltaStep != 0.0f) {
      if (dw > maxDeltaStep)
        dw = maxDeltaStep;
      if (dw < -maxDeltaStep)
        dw = -maxDeltaStep;
    }
    return dw;
  }

  /**
   * calculate the cost of loss function
   *
   * @param sumGrad the sum of grad
   * @param sumHess the sum of hess
   * @return the float
   */
  public float calcGain(float sumGrad, float sumHess) {
    if (sumHess < minChildWeight)
      return 0.0f;
    if (maxDeltaStep == 0.0f) {
      if (regAlpha == 0.0f) {
        return (sumGrad / (sumHess + regLambda)) * sumGrad;
      } else {
        return MathUtils.sqr(MathUtils.thresholdL1(sumGrad, regAlpha)) / (sumHess + regLambda);
      }
    } else {
      float w = calcWeight(sumGrad, sumHess);
      float ret = sumGrad * w + 0.5f * (sumHess + regLambda) * MathUtils.sqr(w);
      if (regAlpha == 0.0f) {
        return -2.0f * ret;
      } else {
        return -2.0f * (ret + regAlpha * Math.abs(w));
      }
    }
  }

  /**
   * calculate cost of loss function with four statistics.
   *
   * @param sumGrad the sum of grad
   * @param sumHess the sum of hess
   * @param testGrad the test grad
   * @param testHess the test hess
   * @return the float
   */
  public float calcGain(float sumGrad, float sumHess, float testGrad, float testHess) {
    float w = calcWeight(sumGrad, sumHess);
    float ret = testGrad * w + 0.5f * (testHess + regLambda) * MathUtils.sqr(w);
    if (regAlpha == 0.0f) {
      return -2.0f * ret;
    } else {
      return -2.0f * (ret + regAlpha * Math.abs(w));
    }
  }

  /**
   * given the loss change, whether we need to invoke pruning.
   *
   * @param lossChg the loss chg
   * @param depth the depth
   * @return the boolean
   */
  public boolean needPrune(float lossChg, int depth) {
    return lossChg < this.minSplitLoss;
  }

  /**
   * whether we can split with current hessian.
   *
   * @param sumHess the sum of hess
   * @param depth the depth
   * @return the boolean
   */
  public boolean cannotSplit(float sumHess, int depth) {
    return sumHess < this.minChildWeight * 2.0f;
  }


  /**
   * maximum sketch size.
   *
   * @return the int
   */
  public int maxSketchSize() {
    int ret = (int) (sketchRatio / sketchEps);
    return ret;
  }
}
