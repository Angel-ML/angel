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

package com.tencent.angel.spark.ml.psf.ftrl;

import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.utils.VectorUtils;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MultiRowUpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.MultiRowUpdateParam.MultiRowPartitionUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;

public class ComputeW extends MultiRowUpdateFunc {

  public ComputeW(int matrixId, int[] rowIds, double[][] values) {
    super(matrixId, rowIds, values);
  }

  public ComputeW(int matrixId, double alpha, double beta, double lambda1, double lambda2) {
    this(matrixId, new int[]{0, 1, 2}, new double[][]{{alpha, beta, lambda1, lambda2}});
  }

  public ComputeW() {}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof MultiRowPartitionUpdateParam) {
      MultiRowPartitionUpdateParam param = (MultiRowPartitionUpdateParam) partParam;
      int[] rowIds = param.getRowIds();
      double[][] values = param.getValues();
      double alpha = values[0][0];
      double beta  = values[0][1];
      double lambda1 = values[0][2];
      double lambda2 = values[0][3];

      ServerPartition part = psContext.getMatrixStorageManager().getPart(param.getPartKey());
      Vector z = part.getRow(rowIds[0]).getSplit();
      Vector n = part.getRow(rowIds[1]).getSplit();
      Vector w = Ufuncs.ftrlthreshold(z, n, alpha, beta, lambda1, lambda2);
      part.getRow(rowIds[2]).setSplit(w.filter(1e-11));

      // calculate bias
      if (param.getPartKey().getStartCol() <= 0 && param.getPartKey().getEndCol() > 0) {
        double zVal = VectorUtils.getDouble(z, 0);
        double nVal = VectorUtils.getDouble(n, 0);
        VectorUtils.setFloat(w, 0, (float) (-1.0 * alpha * zVal / (beta + Math.sqrt(nVal))));
      }
    }
  }

  @Override
  public void update(ServerRow row, double[] values) {
    // Do nothing.
  }
}
