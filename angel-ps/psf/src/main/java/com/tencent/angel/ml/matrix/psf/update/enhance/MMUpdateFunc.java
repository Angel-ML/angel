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


import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;

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

  public MMUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = PSContext.get().getMatrixPartitionManager()
        .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      MMUpdateParam.MMPartitionUpdateParam vs2 =
          (MMUpdateParam.MMPartitionUpdateParam) partParam;
      int[] rowIds = vs2.getRowIds();
      if (Utils.withinPart(partParam.getPartKey(), rowIds)) {
        ServerRow[] rows = new ServerRow[rowIds.length];
        for (int i = 0; i < rowIds.length; i++) {
          rows[i] = part.getRow(rowIds[i]);
        }
        update(rows, vs2.getScalars());
      }
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
      default:
        throw new RuntimeException("currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows, double[] scalars);

}
