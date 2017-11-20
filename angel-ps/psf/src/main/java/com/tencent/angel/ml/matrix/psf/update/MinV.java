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

package com.tencent.angel.ml.matrix.psf.update;

import com.tencent.angel.ml.matrix.psf.update.enhance.MUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.nio.DoubleBuffer;
import java.util.Map;

/**
 * `MinV` is find the minimum value of each element in `fromId1` row and `fromId2`
 */
public class MinV extends MUpdateFunc {

  public MinV(int matrixId, int fromId1, int fromId2, int toId) {
    super(matrixId, new int[]{fromId1, fromId2, toId});
  }

  public MinV() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows) {
    try {
      rows[2].getLock().writeLock().lock();
      DoubleBuffer from1 = rows[0].getData();
      DoubleBuffer from2 = rows[1].getData();
      DoubleBuffer to = rows[2].getData();
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        to.put(i, Math.min(from1.get(i), from2.get(i)));
      }
    } finally {
      rows[2].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows) {
    Long2DoubleOpenHashMap from1 = rows[0].getIndex2ValueMap();
    Long2DoubleOpenHashMap from2 = rows[1].getIndex2ValueMap();

    Long2DoubleOpenHashMap to = from1.clone();
    double defaultValue = Math.min(from1.defaultReturnValue(), from2.defaultReturnValue());
    to.defaultReturnValue(defaultValue);

    for (Map.Entry<Long, Double> entry: to.long2DoubleEntrySet()) {
      if (entry.getValue() > defaultValue) {
        entry.setValue(defaultValue);
      }
    }

    for (Map.Entry<Long, Double> entry: from2.long2DoubleEntrySet()) {
      if (entry.getValue() < to.get(entry.getKey().longValue())) {
        to.put(entry.getKey(), entry.getValue());
      }
    }

    rows[2].setIndex2ValueMap(to);
  }
}
