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

import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;

public class PGDUpdateFunc extends OptMMUpdateFunc {

  public PGDUpdateFunc(int matId, int factor, double lr, double l1RegParam, double l2RegParam) {
    super(matId, new int[] {factor}, new double[] {lr, l1RegParam, l2RegParam});
  }

  public PGDUpdateFunc(int matId, int factor, double lr, double l1RegParam) {
    this(matId, factor, lr, l1RegParam, 0.0);
  }

  public PGDUpdateFunc() {
  }

  public void partitionUpdate(PartitionUpdateParam partParam) {

    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    assert part != null;

    MMUpdateParam.MMPartitionUpdateParam vs2 = (MMUpdateParam.MMPartitionUpdateParam) partParam;
    int[] rowIds = vs2.getRowIds();
    int factor = rowIds[0];

    double[] scalars = vs2.getScalars();

    int totalRowLeng = factor * 2;
    Vector[] doubles = new Vector[totalRowLeng];
    for (int f = 0; f < totalRowLeng; f++) {
      doubles[f] = part.getRow(f).getSplit();
    }
    update(doubles, factor, scalars);

  }

  void update(Vector[] rows, int factor, double[] scalars) {
    double lr = scalars[0];
    double l1RegParam = scalars[1];
    double l2RegParam = scalars[2];

    for (int f = 0; f < factor; f++) {
      Vector weight = rows[f];
      Vector gradient = rows[f + factor];

      // gradient descent first, then truncated
      if (l2RegParam == 0.0) {
        weight.iaxpy(gradient, -lr);
      } else {
        weight.imul(1 - l2RegParam).iaxpy(gradient, -lr);
      }

      Ufuncs.isoftthreshold(weight, lr * l1RegParam);
      gradient.clear();
    }
  }

}
