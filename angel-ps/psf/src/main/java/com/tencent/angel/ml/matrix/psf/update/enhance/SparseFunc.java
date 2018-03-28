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

import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

public abstract class SparseFunc extends UpdateFunc {
  public SparseFunc(int matrixId) {
    super(new SparseParam(matrixId));
  }

  public SparseFunc(SparseParam param) {
    super(param);
  }

  public SparseFunc() {
    super(null);
  }


  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());
    SparseParam.SparseFPartitionUpdateParam param = (SparseParam.SparseFPartitionUpdateParam)partParam;
    int rowId = param.getRowId();
    long[] cols = param.getColIds();
    double[] values = param.getValues();

    if (part != null && Utils.withinPart(partParam.getPartKey(), new int[]{rowId})) {
      ServerRow row = part.getRow(rowId);
      if (row != null) {
        update(row, cols, values);
      }
    }
  }

  private void update(ServerRow row, long[] cols, double[] values) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row, cols, values);
        return;
      case T_DOUBLE_SPARSE_LONGKEY:
        doUpdate((ServerSparseDoubleLongKeyRow) row, cols, values);
        return;
      default:
        throw new RuntimeException("currently only supports DoubleDenseRow and SparseDoubleLongKey," +
            " part:" + row.getRowType());
    }
  }
  protected abstract void doUpdate(ServerDenseDoubleRow row, long[] cols, double[] values);

  protected abstract void doUpdate(ServerSparseDoubleLongKeyRow row, long[] cols, double[] values);
}
