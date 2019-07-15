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


package com.tencent.angel.ml.psf.compress;


import com.tencent.angel.ml.psf.compress.QuantifyDoubleParam.QuantifyDoublePartUParam;
import com.tencent.angel.ml.math2.storage.IntDoubleDenseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerRow;


public class QuantifyDoubleFunc extends UpdateFunc {

  public QuantifyDoubleFunc(int matrixId, int rowId, double[] array, int bitPerItem) {
    super(new QuantifyDoubleParam(matrixId, rowId, array, bitPerItem));
  }

  public QuantifyDoubleFunc(int matrixId, int rowId, IntDoubleVector vector, int bitPerItem) {
    this(matrixId, rowId, vector.getStorage().getValues(), bitPerItem);
  }

  public QuantifyDoubleFunc(int matrixId, int rowId, Vector vector, int bitPerItem) {
    this(matrixId, rowId, (IntDoubleVector) vector, bitPerItem);
  }

  public QuantifyDoubleFunc() {
    super(null);
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      QuantifyDoublePartUParam cp =
        (QuantifyDoublePartUParam) partParam;
      ServerRow row = part.getRow(cp.getRowId());
      if (row != null) {
        update(row, cp.getArraySlice());
      }
    }
  }

  private void update(ServerRow row, double[] arraySlice) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerIntDoubleRow) row, arraySlice);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  private void doUpdate(ServerIntDoubleRow row, double[] arraySlice) {
    try {
      row.getLock().writeLock().lock();
      double[] values = ((IntDoubleDenseVectorStorage)(row.getSplit().getStorage())).getValues();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        values[i] = values[i] + arraySlice[i];
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

}
