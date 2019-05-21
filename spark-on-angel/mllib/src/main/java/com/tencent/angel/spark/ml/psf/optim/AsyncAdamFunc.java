package com.tencent.angel.spark.ml.psf.optim;

import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.ufuncs.OptFuncs;
import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class AsyncAdamFunc extends AsyncOptimFunc {

  public AsyncAdamFunc(UpdateParam param) {
    super(param);
  }

  public AsyncAdamFunc() {
    super(null);
  }

  @Override
  public void update(Vector[] vectors, Vector grad, double[] doubles, int[] ints) {
    double eta = doubles[0];
    double gamma = doubles[1];
    double beta = doubles[2];

    int numUpdates = ints[2];
    double powBeta = Math.pow(beta, numUpdates);
    double powGamma = Math.pow(gamma, numUpdates);

    Vector weight = vectors[0];
    Vector velocity = vectors[1];
    Vector square = vectors[2];

    OptFuncs.iexpsmoothing(velocity, grad, beta);
    OptFuncs.iexpsmoothing2(square, grad, gamma);

    velocity = Ufuncs.indexget(velocity, grad);
//    square = Ufuncs.indexget(square, grad);

    Vector delta = OptFuncs.adamdelta(velocity, square, powBeta, powGamma);
    delta.imul(eta);
    weight.isub(delta);
  }
}
