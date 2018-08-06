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


import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.DoubleBuffer;

public class CompressUpdateFunc extends UpdateFunc {

  private static final Log LOG = LogFactory.getLog(CompressUpdateFunc.class);

  public CompressUpdateFunc(int matrixId, int rowId, double[] array, int bitPerItem) {
    super(new CompressUpdateParam(matrixId, rowId, array, bitPerItem));
  }

  public CompressUpdateFunc(int matrixId, int rowId, DenseDoubleVector vector, int bitPerItem) {
    this(matrixId, rowId, vector.getValues(), bitPerItem);
  }

  public CompressUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part =
      psContext.getMatrixStorageManager()
            .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      CompressUpdateParam.CompressPartitionUpdateParam cp =
          (CompressUpdateParam.CompressPartitionUpdateParam) partParam;
      ServerRow row = part.getRow(cp.getRowId());
      if (row != null) {
        update(row, cp.getArraySlice());
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

  private void doUpdate(ServerDenseDoubleRow row, double[] arraySlice) {
    try {
      row.getLock().writeLock().lock();
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, data.get(i) + arraySlice[i]);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

}
