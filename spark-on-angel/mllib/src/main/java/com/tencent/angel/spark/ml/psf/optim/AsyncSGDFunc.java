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
package com.tencent.angel.spark.ml.psf.optim;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;

public class AsyncSGDFunc extends AsyncOptimFunc {

  public AsyncSGDFunc(UpdateParam param) {
    super(param);
  }

  public AsyncSGDFunc() {
    this(null);
  }

  @Override
  public void update(Vector[] vectors, Vector grad, double[] doubles, int[] ints) {
    double stepSize = doubles[0];
    vectors[0].isub(grad.imul(stepSize));
  }
}
