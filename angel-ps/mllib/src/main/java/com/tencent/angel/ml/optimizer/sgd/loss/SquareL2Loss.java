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

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SquareL2Loss extends L2Loss {
  private static final Log LOG = LogFactory.getLog(SquareL2Loss.class);


  public SquareL2Loss(double _lambda) {
    this.lambda = _lambda;
  }

  public SquareL2Loss() {
  }

  @Override public double loss(double dot, double y) {
    return (dot - y) * (dot - y) / 2.0;
  }

  @Override public double grad(double dot, double y) {
    return (y - dot);
  }

  @Override public double predict(TDoubleVector w, TVector x) {
    return w.dot(x);
  }

}
