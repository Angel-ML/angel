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

package com.tencent.angel.ml.treemodels.param;

import com.tencent.angel.ml.utils.Maths;

public class GBDTParam extends RegTParam {
  public int numTree;  // number of trees
  public int numThread;  // parallelism
  public int batchNum;  // number of batch in mini-batch histogram building

  public float minChildWeight;  // minimum amout of hessian (weight) allowed for a child
  public float regAlpha;  // L1 regularization factor
  public float regLambda;  // L2 regularization factor
  public float maxLeafWeight; // maximum leaf weight, default 0 means no constraints

  /**
   * Calculate leaf weight given the statistics
   *
   * @param sumGrad sum of gradient values
   * @param sumHess sum of hessian values
   * @return weight
   */
  public float calcWeight(float sumGrad, float sumHess) {
    if (sumHess < minChildWeight) {
      return 0.0f;
    }
    float dw;
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

  /**
   * Calculate the cost of loss function
   *
   * @param sumGrad sum of gradient values
   * @param sumHess sum of hessian values
   * @return loss gain
   */
  public float calcGain(float sumGrad, float sumHess) {
    if (sumHess < minChildWeight) {
      return 0.0f;
    }
    if (maxLeafWeight == 0.0f) {
      if (regAlpha == 0.0f) {
        return (sumGrad / (sumHess + regLambda)) * sumGrad;
      } else {
        return Maths.sqr(Maths.thresholdL1(sumGrad, regAlpha)) / (sumHess + regLambda);
      }
    } else {
      float w = calcWeight(sumGrad, sumHess);
      float ret = sumGrad * w + 0.5f * (sumHess + regLambda) * Maths.sqr(w);
      if (regAlpha == 0.0f) {
        return -2.0f * ret;
      } else {
        return -2.0f * (ret + regAlpha * Math.abs(w));
      }
    }
  }

}
