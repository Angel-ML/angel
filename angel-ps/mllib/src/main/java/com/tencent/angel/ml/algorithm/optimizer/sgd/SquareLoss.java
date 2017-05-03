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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.optimizer.sgd;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;

public class SquareLoss implements Loss {

  @Override
  public double loss(double dot, double y) {
    return (dot - y) * (dot - y) / 2.0;
  }

  @Override
  public double grad(double dot, double y) {
    return (y - dot);
  }

  @Override
  public double predict(TDoubleVector w, TAbstractVector x) {
    return w.dot(x);
  }

  @Override
  public boolean isL2Reg() {
    return false;
  }

  @Override
  public boolean isL1Reg() {
    return false;
  }

  @Override
  public double getRegParam() {
    return 0;
  }

  @Override
  public double getReg(TDoubleVector w) {
    return 0;
  }

}
