/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;

/**
 * `FullUpdateFunc` is a PSF Update for the whole matrix in matrix with a user-defined function.
 */

public abstract class FullUpdateFunc extends UpdateFunc {
  public FullUpdateFunc(int matrixId, double[] values) {
    super(new FullUpdateParam(matrixId, values));
  }

  public FullUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext
        .getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      FullUpdateParam.FullPartitionUpdateParam ff =
          (FullUpdateParam.FullPartitionUpdateParam) partParam;

      update(part, partParam.getPartKey(), ff.getValues());
    }
  }

  private void update(ServerPartition part, PartitionKey key, double[] values) {
    switch (part.getRowType()) {
      case T_DOUBLE_DENSE:
        int startRow = key.getStartRow();
        int endRow = key.getEndRow();
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[endRow - startRow];
        for (int i = startRow; i < endRow; i++) {
          denseRows[i - startRow] = (ServerDenseDoubleRow) part.getRow(i);
        }
        doUpdate(denseRows, values);
        return;
      default:
        throw new RuntimeException("currently only supports DoubleDenseRow");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows, double[] values);

}
