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
 * `Ceil` is a implement of `math.ceil` for row in matrix.
 */
public class Ceil extends MUpdateFunc {

  public Ceil(int matrixId, int fromId, int toId) {
    super(matrixId, new int[]{fromId, toId});
  }

  public Ceil() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows) {
    try {
      rows[1].getLock().writeLock().lock();
      DoubleBuffer from = rows[0].getData();
      DoubleBuffer to = rows[1].getData();
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        to.put(i, Math.ceil(from.get(i)));
      }
    } finally {
      rows[1].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows) {
    Long2DoubleOpenHashMap from = rows[0].getIndex2ValueMap();

    Long2DoubleOpenHashMap to = from.clone();

    to.defaultReturnValue(Math.ceil(to.defaultReturnValue()));
    for (Map.Entry<Long, Double> entry : to.long2DoubleEntrySet()) {
      entry.setValue(Math.ceil(entry.getValue()));
    }
  }
}
