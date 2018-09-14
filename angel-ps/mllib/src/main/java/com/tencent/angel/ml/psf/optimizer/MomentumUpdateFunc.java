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

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;

public class MomentumUpdateFunc extends OptMMUpdateFunc {

  public MomentumUpdateFunc(int matId, int offset, double momentum, double lr, double regParam) {
    super(matId, new int[] {offset}, new double[] {momentum, lr, regParam, 1});
  }

  public MomentumUpdateFunc(int matId, int offset, double momentum, double lr, double regParam, int batchSize) {
    super(matId, new int[] {offset}, new double[] {momentum, lr, regParam, batchSize});
  }

  public MomentumUpdateFunc(int matId, int offset, double momentum, double lr) {
    this(matId, offset, momentum, lr, 0.0);
  }

  public MomentumUpdateFunc() {
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    MMUpdateParam.MMPartitionUpdateParam param = (MMUpdateParam.MMPartitionUpdateParam) partParam;

    int[] rowIds = param.getRowIds();
    int offset = rowIds[0];

    double[] scalars = param.getScalars();
    double momentum = scalars[0];
    double stepSize = scalars[1];
    double regParam = scalars[2];
    double batchSize = scalars[3];

    update(part, offset, momentum, stepSize, regParam, batchSize);

  }

  private void update(ServerPartition partition, int offset,
                      double momentum, double stepSize,
                      double regParam, double batchSize) {
    for (int f = 0; f < offset; f++) {
      Vector weight = partition.getRow(f).getSplit();
      Vector velocity = partition.getRow(f + offset).getSplit();
      Vector gradient = partition.getRow(f + 2 * offset).getSplit();

      if (batchSize > 1)
        gradient.idiv(batchSize);

      velocity.imul(momentum).iaxpy(gradient, stepSize);
      if (regParam == 0.0) {
        weight.isub(velocity);
      } else {
        weight.imul(1 - stepSize * regParam).isub(velocity);
      }
      gradient.clear();
    }
  }

}
