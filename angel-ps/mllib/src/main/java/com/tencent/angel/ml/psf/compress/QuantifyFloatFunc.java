package com.tencent.angel.ml.psf.compress;

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


import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.psf.compress.QuantifyFloatParam.QuantifyFloatPartParam;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;


public class QuantifyFloatFunc extends UpdateFunc {

  public QuantifyFloatFunc(int matrixId, int rowId, float[] array, int bitPerItem) {
    super(new QuantifyFloatParam(matrixId, rowId, array, bitPerItem));
  }

  public QuantifyFloatFunc(int matrixId, int rowId, IntFloatVector vector, int bitPerItem) {
    this(matrixId, rowId, vector.getStorage().getValues(), bitPerItem);
  }

  public QuantifyFloatFunc(int matrixId, int rowId, Vector vector, int bitPerItem) {
    this(matrixId, rowId, (IntFloatVector) vector, bitPerItem);
  }

  public QuantifyFloatFunc() {
    super(null);
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    RowBasedPartition part = (RowBasedPartition)psContext.getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      QuantifyFloatPartParam cp =
          (QuantifyFloatPartParam) partParam;
      ServerRow row = part.getRow(cp.getRowId());
      if (row != null) {
        update(row, cp.getArraySlice());
      }
    }
  }

  private void update(ServerRow row, float[] arraySlice) {
    switch (row.getRowType()) {
      case T_FLOAT_DENSE:
        doUpdate((ServerIntFloatRow) row, arraySlice);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  private void doUpdate(ServerIntFloatRow row, float[] arraySlice) {
    try {
      row.getLock().writeLock().lock();
      float[] values = ServerRowUtils.getVector(row).getStorage().getValues();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        values[i] = values[i] + arraySlice[i];
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}

