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
  protected List<Map.Entry<Long, Double>> doProcess(ServerDenseDoubleRow row, long[] cols) {
    Map<Long, Double> result = new HashMap<>();
    for (long colId: cols) {
      double value = row.getData().get((int)colId);
      result.put(colId, value);
    }
    return new ArrayList<>(result.entrySet());
  }

  @Override
  protected List<Map.Entry<Long, Double>> doProcess(ServerSparseDoubleLongKeyRow row, long[] cols) {
    Map<Long, Double> result = new HashMap<>();
    for (long colId : cols) {
      double value = row.getIndex2ValueMap().get(colId);
      result.put(colId, value);
    }
    return new ArrayList<>(result.entrySet());
  }


  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    ArrayList<Long> cols = new ArrayList<>();
    ArrayList<Double> result = new ArrayList<>();

    for (PartitionGetResult part: partResults) {
      ArrayPartitionAggrResult partResult = (ArrayPartitionAggrResult) part;
      long[] keys = partResult.getCols();
      double[] values = partResult.getResult();
      assert (keys.length == values.length);

      for (int i = 0; i < keys.length; i++) {
        cols.add(keys[i]);
        result.add(values[i]);
      }
    }

    return new ArrayAggrResult(Utils.longListToArray(cols), Utils.doubleListToArray(result));
  }
}
