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

import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;
import java.util.List;

/**
 * The Aggregate function `Asum` will return the sum absolute value of the `rowId` row in
 * `matrixId` matrix. For example, if the content of `rowId` row in `matrixId` matrix is
 * [0.3, -11.0, 2.0, 10.1], the aggregate result of `Asum` is 0.3 + 11.0 + 2.0 + 10.1 .
 */

public final class Asum extends UnaryAggrFunc {

  public Asum(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Asum() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double asum = 0.0;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      asum += Math.abs(data.get(i));
    }
    return asum;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    double sum = 0.0;
    for (PartitionGetResult partResult : partResults) {
      if (partResult != null) {
        sum += ((ScalarPartitionAggrResult) partResult).result;
      }
    }

    return new ScalarAggrResult(sum);
  }

}
