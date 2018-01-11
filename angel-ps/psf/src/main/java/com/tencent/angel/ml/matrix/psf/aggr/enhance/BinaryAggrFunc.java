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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

/**
 * This is abstract class of Binary Aggregate Function of POF (PS Oriented Function),
 * other aggregate function will extend `BinaryAggrFunc` and implement `doProcessRow`.
 * This function will process two rows in the same matrix.
 */
public abstract class BinaryAggrFunc extends GetFunc {

  public BinaryAggrFunc(int matrixId, int rowId1, int rowId2) {
    super(new BinaryAggrParam(matrixId, rowId1, rowId2));
  }

  public BinaryAggrFunc() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partKey) {
    ServerPartition part =
        psContext.getMatrixStorageManager()
            .getPart(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());
    int rowId1 = ((BinaryAggrParam.BinaryPartitionAggrParam) partKey).getRowId1();
    int rowId2 = ((BinaryAggrParam.BinaryPartitionAggrParam) partKey).getRowId2();

    if (Utils.withinPart(partKey.getPartKey(), new int[]{rowId1, rowId2})) {
      if (part != null) {
        ServerRow row1 = part.getRow(rowId1);
        ServerRow row2 = part.getRow(rowId2);
        if (row1 != null && row2 != null) {
          double result = processRows(row1, row2);
          return new ScalarPartitionAggrResult(result);
        }
      }
    }
    return null;
  }

  private double processRows(ServerRow row1, ServerRow row2) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        return doProcessRow((ServerDenseDoubleRow) row1, (ServerDenseDoubleRow) row2);
      case T_DOUBLE_SPARSE_LONGKEY:
        return doProcessRow((ServerSparseDoubleLongKeyRow) row1, (ServerSparseDoubleLongKeyRow) row2);
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract double doProcessRow(ServerDenseDoubleRow row1, ServerDenseDoubleRow row2);

  protected abstract double doProcessRow(ServerSparseDoubleLongKeyRow row1, ServerSparseDoubleLongKeyRow row2);

}
