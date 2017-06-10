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
 *
 */

package com.tencent.angel.ml.matrix.psf.aggr;

import com.tencent.angel.ml.matrix.psf.aggr.base.BinaryAggrFunc;
import com.tencent.angel.ml.matrix.psf.aggr.base.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.base.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;
import java.util.List;

/**
 * `Dot` will return dot product result of `rowId1` and `rowId2`.
 * That is math.dot(matrix[rowId1], matrix[rowId2]).
 */
public final class Dot extends BinaryAggrFunc {

  public Dot(int matrixId, int rowId1, int rowId2) {
    super(matrixId, rowId1, rowId2);
  }

  public Dot() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row1, ServerDenseDoubleRow row2) {
    double sum = 0.0;
    DoubleBuffer data1 = row1.getData();
    DoubleBuffer data2 = row2.getData();
    int size = row1.size();
    for (int i = 0; i < size; i++) {
      sum += data1.get(i) * data2.get(i);
    }
    return sum;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    double sum = 0.0;
    for (PartitionGetResult partResult : partResults) {
      sum += ((ScalarPartitionAggrResult) partResult).result;
    }

    return new ScalarAggrResult(sum);
  }

}
