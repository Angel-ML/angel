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
 * `VAUpdaterFunc` is a POF updater for a row in matrix with an array parameter.
 * Constructor's Parameters include int `rowId` and double[] `array`, which correspond to
 * ServerDenseDoubleRow `row` and double[] `array` in `doUpdate` interface respectively.
 */
public abstract class VAUpdaterFunc extends UpdaterFunc {

  public VAUpdaterFunc(int matrixId, int rowId, double[] array) {
    super(new VAUpdaterParam(matrixId, rowId, array));
  }

  public VAUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      VAUpdaterParam.VAPartitionUpdaterParam va =
          (VAUpdaterParam.VAPartitionUpdaterParam) partParam;
      ServerRow row = part.getRow(va.getRowId());
      if (row != null) {
        update(row, va.getArraySlice());
      }
    }
  }

  private void update(ServerRow row, double[] arraySlice) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row, arraySlice);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow row, double[] arraySlice);

}
