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


package com.tencent.angel.ml.matrix.psf.update.update;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import java.util.List;

/**
 * Update PS matrix
 */
public class UpdateRows extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public UpdateRows(UpdateParam param) {
    super(param);
  }

  public UpdateRows() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartUpdateRowsParam partUpdateRowsParam = (PartUpdateRowsParam) partParam;
    List<RowUpdateSplit> updates = partUpdateRowsParam.getUpdates();
    for (RowUpdateSplit update : updates) {
      getVector(partUpdateRowsParam.getMatrixId(), update.getRowId(), partParam.getPartKey())
          .iadd(update.getVector());
    }
  }

  protected Vector getVector(int matrixId, int rowId, PartitionKey part) {
    return psContext.getMatrixStorageManager().getMatrix(matrixId)
        .getPartition(part.getPartitionId()).getRow(rowId).getSplit();
  }
}
