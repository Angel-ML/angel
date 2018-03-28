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

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ps.impl.matrix.*;

/**
 * `MMUpdateFunc` is a POF updater for a row in matrix with multi double parameter.
 *
 * Constructor's Parameters include int[] `rowIds` and double[] `scalars`, which correspond to
 * ServerDenseDoubleRow[] `rows` and double[] `scalars` in `doUpdate` interface respectively.
 *
 * That is the length of `rowIds` and `rows` is exactly the same, rows[i] is the content of
 * rowIds[i] row in matrix.
 */
public abstract class MMUpdateFunc extends UpdateFunc {

  public MMUpdateFunc(int matrixId, int[] rowIds, double[] scalars) {
    super(new MMUpdateParam(matrixId, rowIds, scalars));
  }

  public MMUpdateFunc(int matrixId, int startId, int length, double[] scalars) {
    super(new MMUpdateParam(matrixId, startId, length, scalars));
  }

  public MMUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(
            partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      MMUpdateParam.MMPartitionUpdateParam vs2 =
          (MMUpdateParam.MMPartitionUpdateParam) partParam;
      int[] rowIds = vs2.getRowIds();
      ServerRow[] rows = new ServerRow[rowIds.length];
      for (int i = 0; i < rowIds.length; i++) {
        rows[i] = part.getRow(rowIds[i]);
      }
      update(rows, vs2.getScalars());
    }
  }

  private void update(ServerRow[] rows, double[] scalars) {
    switch (rows[0].getRowType()) {
      case T_DOUBLE_DENSE:
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          denseRows[i] = (ServerDenseDoubleRow) rows[i];
        }
        doUpdate(denseRows, scalars);
        return;
      case T_DOUBLE_SPARSE:
        ServerSparseDoubleRow[] sparseRows = new ServerSparseDoubleRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          sparseRows[i] = (ServerSparseDoubleRow) rows[i];
        }
        doUpdate(sparseRows, scalars);
        return;
      case T_DOUBLE_SPARSE_LONGKEY:
        ServerSparseDoubleLongKeyRow[] sparseLongKeyRows = new ServerSparseDoubleLongKeyRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          sparseLongKeyRows[i] = (ServerSparseDoubleLongKeyRow) rows[i];
        }
        doUpdate(sparseLongKeyRows, scalars);
        return;
      case T_FLOAT_DENSE:
        ServerDenseFloatRow[] denseFloatRows = new ServerDenseFloatRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          denseFloatRows[i] = (ServerDenseFloatRow) rows[i];
        }
        doUpdate(denseFloatRows, scalars);
        return;
      case T_FLOAT_SPARSE:
        ServerSparseFloatRow[] sparseFloatRows = new ServerSparseFloatRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          sparseFloatRows[i] = (ServerSparseFloatRow) rows[i];
        }
        doUpdate(sparseFloatRows, scalars);
        return;
      default:
        throw new RuntimeException("currently only supports T_DOUBLE_DENSE and T_DOUBLE_SPARSE_LONGKEY");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows, double[] scalars);

  protected abstract void doUpdate(ServerSparseDoubleLongKeyRow[] rows, double[] scalars);

  protected void doUpdate(ServerSparseDoubleRow[] rows, double[] scalars) {
    throw new AngelException("Please implement ServerSparseDoubleRow doUpdate frist!");
  }

  protected void doUpdate(ServerDenseFloatRow[] rows, double[] scalars) {
    throw new AngelException("Please implement ServerDenseFloatRow doUpdate frist!");
  }

  protected void doUpdate(ServerSparseFloatRow[] rows, double[] scalars) {
    throw new AngelException("Please implement ServerSparseFloatRow doUpdate frist!");
  }
}
