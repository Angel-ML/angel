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

import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.nio.DoubleBuffer;
import java.util.Map;

/**
 * `MulS` function will multiply `fromId` and `value` and saves to `toId`.
 * That is `toId` = `fromId` * `value`
 */
public class MulS extends MMUpdateFunc {

  public MulS(int matrixId, int fromId, int toId, double value) {
    super(matrixId, new int[]{fromId, toId}, new double[]{value});
  }

  public MulS() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] values) {
    try {
      rows[1].getLock().writeLock().lock();
      DoubleBuffer from = rows[0].getData();
      DoubleBuffer to = rows[1].getData();
      double value = values[0];
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        to.put(i, from.get(i) * value);
      }
    } finally {
      rows[1].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows, double[] scalars) {
    double scalar = scalars[0];
    Long2DoubleOpenHashMap from = rows[0].getData();
    Long2DoubleOpenHashMap to = from.clone();
    to.defaultReturnValue(from.defaultReturnValue() * scalar);


    for (Map.Entry<Long, Double> entry: to.long2DoubleEntrySet()) {
      to.put(entry.getKey().longValue(), scalar * entry.getValue());
    }

    rows[1].setIndex2ValueMap(to);
  }


}
