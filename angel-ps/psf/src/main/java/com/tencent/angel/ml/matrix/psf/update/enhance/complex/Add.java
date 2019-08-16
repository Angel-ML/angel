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
package com.tencent.angel.ml.matrix.psf.update.enhance.complex;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRows;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam;
import com.tencent.angel.ml.matrix.psf.update.update.PartIncrementRowsParam;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

import java.util.List;

public class Add extends IncrementRows {

  public Add(IncrementRowsParam param) {
    super(param);
  }

  public Add() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartIncrementRowsParam param = (PartIncrementRowsParam) partParam;
    List<RowUpdateSplit> updates = param.getUpdates();
    for (RowUpdateSplit update: updates) {
      ServerRow row = psContext.getMatrixStorageManager().getRow(param.getPartKey(), update.getRowId());
      row.startWrite();
      try {
        Vector vector = getVector(param.getMatrixId(), update.getRowId(), param.getPartKey());
        vector.iadd(update.getVector());
      } finally {
        row.endWrite();
      }
    }
  }
}
