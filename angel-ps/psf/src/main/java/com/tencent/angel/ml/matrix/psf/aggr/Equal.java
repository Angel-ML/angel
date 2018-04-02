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

import com.tencent.angel.ml.matrix.psf.aggr.enhance.BinaryAggrFunc;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSemiIndirectHeaps;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.nio.DoubleBuffer;
import java.util.List;

/**
 * `Equal` judges if two rows is equal.
 */
public final class Equal extends BinaryAggrFunc {

  public Equal(int matrixId, int rowId1, int rowId2) {
    super(matrixId, rowId1, rowId2);
  }

  public Equal() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row1, ServerDenseDoubleRow row2) {
    double isEqual = 1.0;
    DoubleBuffer data1 = row1.getData();
    DoubleBuffer data2 = row2.getData();
    int size = row1.size();
    for (int i = 0; i < size; i++) {
      if (Math.abs(data1.get(i) - data2.get(i)) > 1e-10) {
        isEqual = 0.0;
        break;
      }
    }
    return isEqual;
  }

  @Override
  protected double doProcessRow(ServerSparseDoubleLongKeyRow row1, ServerSparseDoubleLongKeyRow row2) {
    Long2DoubleOpenHashMap data1 = row1.getIndex2ValueMap();
    Long2DoubleOpenHashMap data2 = row2.getIndex2ValueMap();

    if (data1.defaultReturnValue() != data2.defaultReturnValue()) {
      return 0.0;
    }
    LongSet keys = new LongOpenHashSet(data1.keySet());
    keys.addAll(data2.keySet());

    if (keys.size() != data1.keySet().size() || keys.size() != data2.keySet().size()) {
      return 0.0;
    }

    for (long key: keys) {
      if (Math.abs(data1.get(key) - data2.get(key)) > 1e-11) {
        return 0.0;
      }
    }
    return 1.0;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    double isEqual = 1.0;
    for (PartitionGetResult partResult : partResults) {
      if (partResult != null) {
        double res = ((ScalarPartitionAggrResult) partResult).result;
        if (res != 1.0) {
          isEqual = res;
          break;
        }
      }
    }

    return new ScalarAggrResult(isEqual);
  }

}
