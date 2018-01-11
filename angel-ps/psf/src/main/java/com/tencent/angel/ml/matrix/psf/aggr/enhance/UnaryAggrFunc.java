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
 * This is abstract class of Unary Aggregate Function of POF (PS Oriented Function),
 * other aggregate function will extend `UnaryAggrFunc` and implement `doProcessRow`.
 */
public abstract class UnaryAggrFunc extends GetFunc {

  /**
   * `UnaryAggrFunc` must specify the `matrixId` and `rowId`, the `UnaryAggrFunc` will
   * be apply to this `rowId` row of this `matrixId` matrix.
   *
   * @param matrixId to deal with
   * @param rowId to deal with
   */
  public UnaryAggrFunc(int matrixId, int rowId) {
    super(new UnaryAggrParam(matrixId, rowId));
  }

  public UnaryAggrFunc() {
    this(-1, -1);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partKey) {
    ServerPartition part =
      psContext.getMatrixStorageManager()
            .getPart(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());

    if (part != null) {
      int rowId = ((UnaryAggrParam.UnaryPartitionAggrParam) partKey).getRowId();
      if (Utils.withinPart(part.getPartitionKey(), new int[]{rowId})) {
        ServerRow row = part.getRow(rowId);
        double result = processRow(row);
        return new ScalarPartitionAggrResult(result);
      }
    }
    return null;
  }

  private double processRow(ServerRow row) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        return doProcessRow((ServerDenseDoubleRow) row);
      case T_DOUBLE_SPARSE_LONGKEY:
        return doProcessRow((ServerSparseDoubleLongKeyRow) row);
      default:
        throw new RuntimeException(this.getClass().getName() +
            "currently only supports DoubleDense and SparseDoubleLong Row");
    }
  }

  /**
   * The specify function for `ServerDenseDoubleRow`.
   * @param row to process
   * @return aggregated result of each partition
   */
  protected abstract double doProcessRow(ServerDenseDoubleRow row);

  /**
   * The process function for `ServerSparseDoubleLongKeyRow`.
   * @param row to process
   * @return aggregated result of each partition
   */
  protected abstract double doProcessRow(ServerSparseDoubleLongKeyRow row);

}
