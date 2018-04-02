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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

import java.util.List;
import java.util.Map;

/**
 * This is abstract class of Matrix Aggregate Function of PSF (PS Function),
 * other aggregate function will extend `FullAggrFunc` and implement `doProcessRow`.
 * This function will process the whole matrix.
 */
public abstract class ArrayAggrFunc extends GetFunc {

  public ArrayAggrFunc(int matrixId, int rowId, long[] cols) {
    super(new ArrayAggrParam(matrixId, rowId, cols));
  }

  public ArrayAggrFunc() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partKey) {
    ServerPartition part =
        psContext.getMatrixStorageManager()
            .getPart(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());

    if (part != null) {
      int rowId = ((ArrayAggrParam.ArrayPartitionAggrParam) partKey).getRowId();
      if (Utils.withinPart(part.getPartitionKey(), new int[]{rowId})) {
        ServerRow row = part.getRow(rowId);
        long[] colsParam = ((ArrayAggrParam.ArrayPartitionAggrParam) partKey).getCols();

        return processRow(row, colsParam);
      }
    }
    return null;
  }

  private ArrayPartitionAggrResult processRow(ServerRow row, long[] cols) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        return doProcess((ServerDenseDoubleRow) row, cols);
      case T_DOUBLE_SPARSE_LONGKEY:
        return doProcess((ServerSparseDoubleLongKeyRow) row, cols);
      default:
        throw new RuntimeException(this.getClass().getName() +
            "currently only supports DoubleDense and SparseDoubleLong Row");
    }
  }

  protected abstract ArrayPartitionAggrResult doProcess(ServerDenseDoubleRow row, long[] cols);

  protected abstract ArrayPartitionAggrResult doProcess(ServerSparseDoubleLongKeyRow row, long[] cols);

}
