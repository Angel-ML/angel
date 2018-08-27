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

public class SGDUpdateFunc extends OptMMUpdateFunc {
  public SGDUpdateFunc(int matId, int factor, double lr, double l2RegParam) {
    super(matId, new int[] {factor}, new double[] {lr, l2RegParam});
  }

  public SGDUpdateFunc(int matId, int factor, double lr) {
    this(matId, factor, lr, 0.0);
  }

  public SGDUpdateFunc() {
  }

  public void partitionUpdate(PartitionUpdateParam partParam) {

    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    assert part != null;

    MMUpdateParam.MMPartitionUpdateParam vs2 = (MMUpdateParam.MMPartitionUpdateParam) partParam;
    int[] rowIds = vs2.getRowIds();
    int offset = rowIds[0];

    double[] scalars = vs2.getScalars();

    double stepSize = scalars[0];
    double regParam = scalars[1];

    update(part, offset, stepSize, regParam);
  }

  private void update(ServerPartition partition, int offset, double stepSize, double regParam) {
    for (int f = 0; f < offset; f++) {
      Vector weight = partition.getRow(f).getSplit();
      Vector gradient = partition.getRow(f + offset).getSplit();
      if (regParam == 0.0)
        weight.iaxpy(gradient, -stepSize);
      else
        weight.imul(1 - regParam * stepSize).iaxpy(gradient, -stepSize);
      gradient.clear();
    }
  }
}
