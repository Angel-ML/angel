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

package com.tencent.angel.ml.matrix.psf.aggr;


import com.tencent.angel.ml.matrix.psf.aggr.enhance.ArrayAggrFunc;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ArrayAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ArrayPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

import java.nio.DoubleBuffer;
import java.util.*;

/**
 * `PullWithCols` can pull specific cols in a row.
 */
public class PullWithCols extends ArrayAggrFunc {
  int rowId;
  long[] cols;
  public PullWithCols(int matrixId, int rowId, long[] cols) {
    super(matrixId, rowId, cols);
    this.rowId = rowId;
    this.cols = cols;
  }

  public PullWithCols() {
    super();
  }

  @Override
  protected ArrayPartitionAggrResult doProcess(ServerDenseDoubleRow row, long[] cols) {
    try {
      row.getLock().readLock().lock();
      double[] values = new double[cols.length];
      DoubleBuffer db = row.getData();
      int i = 0;
      for (long colId : cols) {
        values[i++] = db.get((int) (colId - row.getStartCol()));
      }
      return new ArrayPartitionAggrResult(cols, values);
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  @Override
  protected ArrayPartitionAggrResult doProcess(ServerSparseDoubleLongKeyRow row, long[] cols) {
    try {
      row.getLock().readLock().lock();
      double[] values = new double[cols.length];
      int i = 0;
      for (long colId: cols) {
        values[i++] = row.getIndex2ValueMap().get(colId);
      }
      return new ArrayPartitionAggrResult(cols, values);
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int totalLen = 0;
    for (PartitionGetResult part: partResults) {
      ArrayPartitionAggrResult partResult = (ArrayPartitionAggrResult) part;
      totalLen += partResult.getCols().length;
    }

    long[] combCols = new long[totalLen];
    double[] combValues = new double[totalLen];

    int accuIndex = 0;

    for (PartitionGetResult part: partResults) {
      ArrayPartitionAggrResult partResult = (ArrayPartitionAggrResult) part;

      long[] keys = partResult.getCols();
      double[] values = partResult.getResult();
      assert (keys.length == values.length);

      System.arraycopy(keys, 0, combCols, accuIndex, keys.length);
      System.arraycopy(values, 0, combValues, accuIndex, values.length);
      accuIndex += keys.length;
    }

    return new ArrayAggrResult(combCols, combValues);
  }
}
