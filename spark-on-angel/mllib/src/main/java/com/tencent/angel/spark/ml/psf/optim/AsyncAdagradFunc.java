package com.tencent.angel.spark.ml.psf.optim;

import com.tencent.angel.ml.math2.ufuncs.OptFuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;

public class AsyncAdagradFunc extends AsyncOptimFunc {

  public AsyncAdagradFunc(UpdateParam param) {
    super(param);
  }

  public AsyncAdagradFunc() {
    this(null);
  }

  @Override
  public void update(Vector[] vectors, Vector grad, double[] doubles, int[] ints) {
    double eta = doubles[0];
    double factor = doubles[1];

    Vector weight = vectors[0];
    Vector square = vectors[1];

    OptFuncs.iexpsmoothing2(square, grad, factor);
    grad = OptFuncs.adagraddelta(grad, square, 0.0, eta);
    weight.isub(grad);
  }
}
