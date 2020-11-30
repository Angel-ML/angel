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


package com.tencent.angel.ml.lda.psf;


import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerRow;

public class ResetFunc extends UpdateFunc {

  public ResetFunc(UpdateParam param) {
    super(param);
  }

  public ResetFunc() {
    super(null);
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    RowBasedPartition part = (RowBasedPartition)psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        if (row == null) {
          continue;
        }

        row.reset();
//        reset(row);
      }
    }
  }

  private void reset(ServerRow row) {
    switch (row.getRowType()) {
      case T_INT_SPARSE:
      case T_INT_DENSE:
        reset((ServerIntIntRow) row);

      default:
        break;
    }
  }

  private void reset(ServerIntIntRow row) {
    try {
      row.startWrite();
      row.reset();
    } finally {
      row.endWrite();
    }
  }
}
