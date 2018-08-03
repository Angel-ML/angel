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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;

/**
 * This is abstract class of Binary Aggregate Function of POF (PS Oriented Function),
 * other aggregate function will extend `MultiAggrFunc` and implement `doProcessRow`.
 * This function will process two rows in the same matrix.
 */
public abstract class MultiAggrFunc extends GetFunc {

  public MultiAggrFunc(int matrixId, int[] rowIds) {
    super(new MultiAggrParam(matrixId, rowIds));
  }

  public MultiAggrFunc() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partKey) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());
    int[] rowIds = ((MultiAggrParam.MultiPartitionAggrParam) partKey).getRowIds();

    double[] result = null;
    if (Utils.withinPart(partKey.getPartKey(), rowIds)) {
      if (part != null) {
        ServerRow[] rows = new ServerRow[rowIds.length];
        for (int i = 0; i < rowIds.length; i++) {
          rows[i] = part.getRow(rowIds[i]);
        }
        result = processRows(rows);
      }
    }
    return new ArrayPartitionAggrResult(result);
  }

  private double[] processRows(ServerRow[] rows) {
    switch (rows[0].getRowType()) {
      case T_DOUBLE_DENSE:
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
          denseRows[i] = (ServerDenseDoubleRow) rows[i];
        }
        return doProcessRow(denseRows);
      default:
        throw new RuntimeException("currently only supports Double Dense Row");
    }
  }

  protected abstract double[] doProcessRow(ServerDenseDoubleRow[] rows);

}
