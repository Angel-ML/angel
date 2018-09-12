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

public class AdamUpdateFunc extends OptMMUpdateFunc {

  private int sampleNum = 1;

  public AdamUpdateFunc(int matId, int factor, double gamma, double epsilon, double beta, double lr,
    double regParam, double iteration) {
    super(matId, new int[] {factor}, new double[] {gamma, epsilon, beta, lr, regParam, iteration});
  }

  public AdamUpdateFunc(int matId, int factor, double gamma, double epsilon, double beta, double lr,
                        double regParam, double iteration, int sampleNum) {
    super(matId, new int[] {factor}, new double[] {gamma, epsilon, beta, lr, regParam, iteration});
    this.sampleNum = sampleNum;
  }

  public AdamUpdateFunc() {
    super();
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    MMUpdateParam.MMPartitionUpdateParam vs2 = (MMUpdateParam.MMPartitionUpdateParam) partParam;
    int offset = vs2.getRowIds()[0];
    double[] scalars = vs2.getScalars();

    double gamma = scalars[0];
    double epsilon = scalars[1];
    double beta = scalars[2];
    double lr = scalars[3];
    double regParam = scalars[4];
    double iteration = scalars[5];

    update(part, offset, gamma, beta, epsilon, lr, regParam, iteration);

  }

  private void update(ServerPartition partition, int offset, double gamma, double beta,
                         double epsilon, double stepSize, double regParam, double iteration) {
    if (iteration == 0)
      iteration = 1;
    double powBeta = Math.pow(beta, iteration);
    double powGamma = Math.pow(gamma, iteration);

    for (int f = 0; f < offset; f++) {
      Vector weight = partition.getRow(f).getSplit();
      Vector velocity = partition.getRow(f + offset).getSplit();
      Vector square = partition.getRow(f + 2 * offset).getSplit();
      Vector gradient = partition.getRow(f + 3 * offset).getSplit();

      if (sampleNum > 1)
        gradient.idiv(sampleNum);

      OptFuncs.iexpsmoothing(velocity, gradient, beta);
      OptFuncs.iexpsmoothing2(square, gradient, gamma);

      Vector delta = OptFuncs.adamdelta(velocity, square, powBeta, powGamma);
      if (regParam != 0.0) {
        weight.imul(1 - stepSize * regParam);
      }
      weight.iaxpy(delta, -stepSize);
      gradient.clear();
    }
  }

}
