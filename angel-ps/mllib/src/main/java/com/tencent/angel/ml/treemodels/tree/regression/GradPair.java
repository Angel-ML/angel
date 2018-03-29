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

package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.GBDTParam;

public class GradPair {
  private float grad;  // gradient value
  private float hess;  // hessian value

  public GradPair() {
  }

  public GradPair(float grad, float hess) {
    this.grad = grad;
    this.hess = hess;
  }

  public float getGrad() {
    return grad;
  }

  public float getHess() {
    return hess;
  }

  public void setGrad(float grad) {
    this.grad = grad;
  }

  public void setHess(float hess) {
    this.hess = hess;
  }

  public void update(float sumGrad, float sumHess) {
    this.grad = sumGrad;
    this.hess = sumHess;
  }

  public void add(float grad, float hess) {
    this.grad += grad;
    this.hess += hess;
  }

  public void add(GradPair p) {
    this.add(p.grad, p.hess);
  }

  public float calcWeight(GBDTParam param) {
    return param.calcWeight(grad, hess);
  }

  public float calcGain(GBDTParam param) {
    return param.calcGain(grad, hess);
  }
}
