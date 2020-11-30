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

import com.tencent.angel.ml.math2.ufuncs.OptFuncs;
import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.ufuncs.expression.OpType;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;


public class AsyncFTRLFunc extends AsyncOptimFunc {

  public AsyncFTRLFunc(UpdateParam param) {
    super(param);
  }

  public AsyncFTRLFunc() {
    this(null);
  }

  @Override
  public void update(Vector[] vectors, Vector grad, double[] doubles, int[] ints) {
    double alpha = doubles[0];
    double beta = doubles[1];
    double lambda1 = doubles[2];
    double lambda2 = doubles[3];

    Vector w = vectors[0];
    Vector n = vectors[1];
    Vector z = vectors[2];

    Ufuncs.iaxpy2(n, grad, 1);
    Vector delta = OptFuncs.ftrldelta(n, grad, alpha, OpType.INTERSECTION);
    Ufuncs.isub(grad, delta.mul(w));
    Ufuncs.iadd(z, grad);

    // how to do intersection for two dense vector with a given indices ??

  }
}
