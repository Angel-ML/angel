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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;

/**
 * This is abstract class of Matrix Aggregate Function of PSF (PS Function),
 * other aggregate function will extend `FullAggrFunc` and implement `doProcessRow`.
 * This function will process the whole matrix.
 */
public abstract class FullAggrFunc extends GetFunc {

  public FullAggrFunc(int matrixId) {
    super(new FullAggrParam(matrixId));
  }

  public FullAggrFunc() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partKey) {
    ServerPartition part = PSContext.get().getMatrixPartitionManager()
        .getPartition(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());

    double[][] result = null;
    if (part != null) {
      result = process(part, partKey.getPartKey());
    }
    PartitionKey key = partKey.getPartKey();
    int[] partInfo = new int[] {key.getStartRow(), key.getEndRow()
        , key.getStartCol(), key.getEndCol()};
    return new FullPartitionAggrResult(result, partInfo);
  }

  private double[][] process(ServerPartition part, PartitionKey key) {
    switch (part.getRowType()) {
      case T_DOUBLE_DENSE:
        int startRow = key.getStartRow();
        int endRow = key.getEndRow();
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[endRow - startRow];
        for (int i = startRow; i < endRow; i++) {
          denseRows[i - startRow] = (ServerDenseDoubleRow) part.getRow(i);
        }
        return doProcess(denseRows);
      default:
        throw new RuntimeException("currently only supports Double Dense Row");
    }
  }

  protected abstract double[][] doProcess(ServerDenseDoubleRow[] rows);

}
