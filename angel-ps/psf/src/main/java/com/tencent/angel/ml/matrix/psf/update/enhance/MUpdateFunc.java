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
 *
 */

package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

/**
 * `MUpdateFunc` is a POF updater for multi rows in matrix.
 * Constructor's Parameters include int[] `rowIds`, which correspond to
 * ServerDenseDoubleRow[] `rows` in `doUpdate` interface.
 *
 * That is the length of `rowIds` and `rows` is exactly the same, rows[i] is the content of
 * rowIds[i] row in matrix.
 */
public abstract class MUpdateFunc extends UpdateFunc {
  protected MUpdateFunc(int matrixId, int[] rowIds) {
    super(new MUpdateParam(matrixId, rowIds));
  }

  protected MUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      MUpdateParam.MPartitionUpdateParam m = (MUpdateParam.MPartitionUpdateParam) partParam;
      int[] rowIds = m.getRowIds();
      if (Utils.withinPart(partParam.getPartKey(), rowIds)) {
        ServerRow[] rows = new ServerRow[rowIds.length];
        for (int i = 0; i < rowIds.length; i++) {
          rows[i] = part.getRow(rowIds[i]);
        }
        update(rows);
      }
    }
  }

  private void update(ServerRow[] rows) {
    switch (rows[0].getRowType()) {
      case T_DOUBLE_DENSE:
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          denseRows[i] = (ServerDenseDoubleRow) rows[i];
        }
        doUpdate(denseRows);
        return;
      case T_DOUBLE_SPARSE_LONGKEY:
        ServerSparseDoubleLongKeyRow[] sparseRows = new ServerSparseDoubleLongKeyRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          sparseRows[i] = (ServerSparseDoubleLongKeyRow) rows[i];
        }
        doUpdate(sparseRows);
        return;
      default:
        throw new RuntimeException("currently only supports T_DOUBLE_DENSE and T_DOUBLE_SPARSE_LONGKEY");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows);

  protected abstract void doUpdate(ServerSparseDoubleLongKeyRow[] rows);

}
