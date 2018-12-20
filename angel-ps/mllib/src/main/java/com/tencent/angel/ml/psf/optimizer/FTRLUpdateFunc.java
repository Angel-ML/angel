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


package com.tencent.angel.ml.psf.optimizer;

import com.tencent.angel.ml.math2.ufuncs.OptFuncs;
import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;

public class FTRLUpdateFunc extends OptMMUpdateFunc {
  public FTRLUpdateFunc(int matId, int factor, double alpha, double beta, double lambda1,
    double lambda2) {
    super(matId, new int[] {factor}, new double[] {alpha, beta, lambda1, lambda2});
  }

  public FTRLUpdateFunc() {
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    assert part != null;

    MMUpdateParam.MMPartitionUpdateParam vs2 = (MMUpdateParam.MMPartitionUpdateParam) partParam;
    int[] rowIds = vs2.getRowIds();
    int factor = rowIds[0];

    double[] scalars = vs2.getScalars();

    int totalRowLeng = factor * 4;
    Vector[] doubles = new Vector[totalRowLeng];
    for (int f = 0; f < totalRowLeng; f++) {
      doubles[f] = part.getRow(f).getSplit();
    }
    update(doubles, factor, scalars);
  }

  void update(Vector[] rows, int factor, double[] scalars) {
    double alpha = scalars[0];
    double beta = scalars[1];
    double lambda1 = scalars[2];
    double lambda2 = scalars[3];

    for (int f = 0; f < factor; f++) {
      Vector weight = rows[f];
      Vector zModel = rows[f + factor];
      Vector qModel = rows[f + 2 * factor];
      Vector gradient = rows[f + 3 * factor];

      Vector sigma = OptFuncs.ftrldelta(qModel, gradient, alpha);
      Ufuncs.iaxpy2(qModel, gradient, 1);
      zModel.iadd(gradient.sub(sigma.mul(weight)));

      Vector newWeight = Ufuncs.ftrlthreshold(zModel, qModel, alpha, beta, lambda1, lambda2);
      weight.setStorage(newWeight.getStorage());

      gradient.imul(0.0);
    }
  }
}
