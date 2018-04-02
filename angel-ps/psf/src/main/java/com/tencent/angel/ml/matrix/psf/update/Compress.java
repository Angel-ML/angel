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
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.DoubleBuffer;
import java.util.Map;

/**
 * `Abs` applies math.abs to `fromId` row, and save the result to `toId` row.
 */
public class Compress extends MUpdateFunc {

  public Compress(int matrixId, int rowId) {
    super(matrixId, new int[]{rowId});
  }

  public Compress() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows) {

  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows) {
    rows[0].tryToLockWrite();
    try {
      Long2DoubleOpenHashMap data = rows[0].getIndex2ValueMap();
      ObjectIterator<Long2DoubleMap.Entry> iter = data.long2DoubleEntrySet().fastIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        if (Math.abs(entry.getDoubleValue() - data.defaultReturnValue()) < 1e-11) {
          iter.remove();
        }
      }
    } finally {
      rows[0].unlockWrite();
    }
  }

}
