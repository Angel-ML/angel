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


package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.ml.matrix.psf.Utils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerLongDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerRow;

/**
 * `VAUpdateFunc` is a POF updater for a row in matrix with an array parameter.
 * Constructor's Parameters include int `rowId` and double[] `array`, which correspond to
 * ServerDenseDoubleRow `row` and double[] `array` in `doUpdate` interface respectively.
 */
public abstract class VAUpdateFunc extends UpdateFunc {

  public VAUpdateFunc(int matrixId, int rowId, double[] array) {
    super(new VAUpdateParam(matrixId, rowId, array));
  }

  public VAUpdateFunc() {
    super(null);
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      VAUpdateParam.VAPartitionUpdateParam va = (VAUpdateParam.VAPartitionUpdateParam) partParam;
      int rowId = va.getRowId();
      if (Utils.withinPart(partParam.getPartKey(), new int[] {rowId})) {
        ServerRow row = part.getRow(rowId);
        if (row != null) {
          update(row, va.getArray());
        }
      }
    }
  }

  private void update(ServerRow row, double[] arraySlice) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerIntDoubleRow) row, arraySlice);
        return;
      case T_DOUBLE_SPARSE_LONGKEY:
        doUpdate((ServerLongDoubleRow) row, arraySlice);
        return;
      default:
        throw new RuntimeException(
          "currently only supports T_DOUBLE_DENSE and T_DOUBLE_SPARSE_LONGKEY");
    }
  }

  protected abstract void doUpdate(ServerIntDoubleRow row, double[] arraySlice);

  protected abstract void doUpdate(ServerLongDoubleRow row, double[] arraySlice);


}
