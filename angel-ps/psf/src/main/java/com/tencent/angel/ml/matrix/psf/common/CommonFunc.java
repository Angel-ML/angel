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

package com.tencent.angel.ml.matrix.psf.common;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

public abstract class CommonFunc extends UpdateFunc {
  public CommonFunc(int matrixId) {
    super(new CommonParam(matrixId));
  }

  public CommonFunc(UpdateParam param) {
    super(param);
  }

  public CommonFunc() {
    super(null);
  }


  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());
    if (part != null) {
      update(part, partParam.getPartKey(), partParam);
    }
  }

  private void update(ServerPartition part, PartitionKey key, PartitionUpdateParam param) {
    int startRow = key.getStartRow();
    int endRow = key.getEndRow();
    switch (part.getRowType()) {
      case T_DOUBLE_DENSE:
        ServerDenseDoubleRow[] denseRows = new ServerDenseDoubleRow[endRow - startRow];
        for (int i = startRow; i < endRow; i++) {
          denseRows[i - startRow] = (ServerDenseDoubleRow) part.getRow(i);
        }
        doUpdate(denseRows, key, param);
        return;
      case T_DOUBLE_SPARSE_LONGKEY:
        ServerSparseDoubleLongKeyRow[] longKeyRows = new ServerSparseDoubleLongKeyRow[endRow - startRow];
        for (int i = startRow; i < endRow; i++) {
          longKeyRows[i - startRow] = (ServerSparseDoubleLongKeyRow) part.getRow(i);
        }
        doUpdate(longKeyRows, key, param);
        return;
      default:
        throw new RuntimeException("currently only supports DoubleDenseRow and SparseDoubleLongKey," +
            " part:" + part.getRowType());
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow[] rows, PartitionKey key, PartitionUpdateParam values);

  protected abstract void doUpdate(ServerSparseDoubleLongKeyRow[] rows, PartitionKey key, PartitionUpdateParam values);
}
