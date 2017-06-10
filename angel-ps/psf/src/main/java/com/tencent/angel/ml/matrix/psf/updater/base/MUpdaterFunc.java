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

package com.tencent.angel.ml.matrix.psf.updater.base;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;

/**
 * `MUpdaterFunc` is a POF updater for multi rows in matrix.
 * Constructor's Parameters include int[] `rowIds`, which correspond to
 * ServerDenseDoubleRow[] `rows` in `doUpdate` interface.
 *
 * That is the length of `rowIds` and `rows` is exactly the same, rows[i] is the content of
 * rowIds[i] row in matrix.
 */
public abstract class MUpdaterFunc extends UpdaterFunc {
  protected MUpdaterFunc(int matrixId, int[] rowIds) {
    super(new MUpdaterParam(matrixId, rowIds));
  }

  protected MUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part = PSContext.get().getMatrixPartitionManager()
        .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      MUpdaterParam.MPartitionUpdaterParam m = (MUpdaterParam.MPartitionUpdaterParam) partParam;
      int[] rowIds = m.getRowIds();
      ServerRow[] rows = new ServerRow[rowIds.length];
      for (int i = 0; i < rowIds.length; i++) {
        rows[i] = part.getRow(rowIds[i]);
      }
      update(rows);
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
      default:
        throw new RuntimeException("currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows);
}
