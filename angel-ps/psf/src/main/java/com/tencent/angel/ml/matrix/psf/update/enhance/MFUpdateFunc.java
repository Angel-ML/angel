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

package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

/**
 * `MFUpdateFunc` is a POF Update for multi rows in matrix with a user-defined function.
 * Constructor's Parameters include int[] `rowIds` and Serialize `func`, which correspond to
 * ServerDenseDoubleRow[] `rows` and Serialize `func` in `doUpdate` interface respectively.
 *
 * That is the length of `rowIds` and `rows` is exactly the same, rows[i] is the content of
 * rowIds[i] row in matrix.
 */
public abstract class MFUpdateFunc extends UpdateFunc {
  public MFUpdateFunc(int matrixId, int[] rowIds, Serialize func) {
    super(new MFUpdateParam(matrixId, rowIds, func));
  }

  public MFUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext
        .getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      MFUpdateParam.MFPartitionUpdateParam mf = (MFUpdateParam.MFPartitionUpdateParam) partParam;

      int[] rowIds = mf.getRowIds();
      if (Utils.withinPart(partParam.getPartKey(), rowIds)) {
        ServerRow[] rows = new ServerRow[rowIds.length];
        for (int i = 0; i < rowIds.length; i++) {
          rows[i] = part.getRow(rowIds[i]);
        }
        update(rows, mf.getFunc());
      }
    }
  }

  private void update(ServerRow[] rows, Serialize func) {
    switch (rows[0].getRowType()) {
      case T_DOUBLE_DENSE:
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          denseRows[i] = (ServerDenseDoubleRow) rows[i];
        }
        doUpdate(denseRows, func);
        return;
      case T_DOUBLE_SPARSE_LONGKEY:
        ServerSparseDoubleLongKeyRow[] sparseRows = new ServerSparseDoubleLongKeyRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          sparseRows[i] = (ServerSparseDoubleLongKeyRow) rows[i];
        }
        doUpdate(sparseRows, func);
        return;
      default:
        throw new RuntimeException("currently only supports Double Dense Row and Sparse LongKey Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows, Serialize func);

  protected abstract void doUpdate(ServerSparseDoubleLongKeyRow[] rows, Serialize func);

}
