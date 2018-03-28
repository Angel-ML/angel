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
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.DoubleBuffer;
import java.util.List;


/**
 * `Nnz` will return the number of non-zero of the `rowId` row in `matrixId` matrix.
 * For example, if the content of `rowId` row in `matrixId` matrix is [0.0, -11.0, 0.0, 10.1],
 * the aggregate result of `Nnz` is 2 .
 */
public final class Nnz extends UnaryAggrFunc {

  public Nnz(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Nnz() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    int nnz = 0;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      if (data.get(i) != 0) nnz++;
    }
    return nnz;
  }

  @Override
  protected double doProcessRow(ServerSparseDoubleLongKeyRow row) {
    Long2DoubleOpenHashMap data = row.getIndex2ValueMap();
    int nnz = 0;
    ObjectIterator<Long2DoubleMap.Entry> iter  = data.long2DoubleEntrySet().iterator();
    while (iter.hasNext()) {
      Long2DoubleMap.Entry entry = iter.next();
      if (Math.abs(entry.getDoubleValue() - data.defaultReturnValue()) > 1e-11) {
         nnz += 1;
      } else {
        iter.remove();
      }
    }
    return (double)nnz;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int nnz = 0;
    for (PartitionGetResult partResult : partResults) {
      if (partResult != null) {
        nnz += ((ScalarPartitionAggrResult) partResult).result;
      }
    }

    return new ScalarAggrResult(nnz);
  }

}
