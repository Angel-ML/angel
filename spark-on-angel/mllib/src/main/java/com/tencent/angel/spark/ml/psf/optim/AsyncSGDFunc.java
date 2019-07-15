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
