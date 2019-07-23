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
