package com.tencent.angel.spark.ml.psf.optim;

import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import parquet.org.apache.thrift.async.AsyncMethodCallback;

public class AsyncMomentumFunc extends AsyncOptimFunc {

  public AsyncMomentumFunc(UpdateParam param) {
    super(param);
  }

  public AsyncMomentumFunc() {
    this(null);
  }

  @Override
  public void update(Vector[] vectors, Vector grad, double[] doubles, int[] ints) {
    double eta = doubles[0];
    double momentum = doubles[1];

    Vector weight = vectors[0];
    Vector velocity = vectors[1];

    Ufuncs.imomentumupdate(velocity, grad, momentum, eta);
    grad = Ufuncs.indexget(velocity, grad);
    weight.isub(grad);
  }
}
