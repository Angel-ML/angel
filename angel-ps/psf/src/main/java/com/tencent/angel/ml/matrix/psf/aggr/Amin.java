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
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.nio.DoubleBuffer;
import java.util.List;
import java.util.Map;

/**
 * `Amin` will aggregate the minimum absolute value of the `rowId` row in `matrixId` matrix.
 * For example, if the content of `rowId` row in `matrixId` matrix is [0.3, -1.1, 2.0, 10.1],
 * the aggregate result of `Amin` is 0.3.
 */
public final class Amin extends UnaryAggrFunc {

  public Amin(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Amin() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double amin = Double.MAX_VALUE;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      amin = Math.min(amin, Math.abs(data.get(i)));
    }
    return amin;
  }

  @Override
  protected double doProcessRow(ServerSparseDoubleLongKeyRow row) {
    Long2DoubleOpenHashMap data = row.getIndex2ValueMap();

    double amin = Math.abs(data.defaultReturnValue());
    for (Long2DoubleMap.Entry entry: data.long2DoubleEntrySet()) {
      amin = Math.min(amin, Math.abs(entry.getDoubleValue()));
    }
    return amin;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    double min = Double.MAX_VALUE;
    for (PartitionGetResult partResult : partResults) {
      if (partResult != null) {
        min = Math.min(min, ((ScalarPartitionAggrResult) partResult).result);
      }
    }

    return new ScalarAggrResult(min);
  }

}
