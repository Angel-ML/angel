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

/**
 * `Copy` the content in `sourceId` to `targetId`
 */
public class Copy extends MUpdateFunc {

  public Copy(int matrixId, int sourceId, int targetId) {
    super(matrixId, new int[]{sourceId, targetId});
  }

  public Copy() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows) {
    rows[1].tryToLockWrite();
    try {
      DoubleBuffer source = rows[0].getData();
      DoubleBuffer target = rows[1].getData();
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        target.put(i, source.get(i));
      }
    } finally {
      rows[1].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows) {
    Long2DoubleOpenHashMap from = rows[0].getIndex2ValueMap();
    Long2DoubleOpenHashMap to = from.clone();
    rows[1].setIndex2ValueMap(to);
  }
}
