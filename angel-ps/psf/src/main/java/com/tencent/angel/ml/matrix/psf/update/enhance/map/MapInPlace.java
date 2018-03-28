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

package com.tencent.angel.ml.matrix.psf.update.enhance.map;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.matrix.psf.update.enhance.MFUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.DoubleBuffer;

/**
 * It is a Map function which applies `MapFunc` to `fromId` row and saves the result to `toId` row
 */
public class MapInPlace extends MFUpdateFunc {

  public MapInPlace(int matrixId, int rowId, MapFunc func) {
    super(matrixId, new int[]{rowId}, func);
  }

  public MapInPlace() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, Serialize func) {
    MapFunc mapper = (MapFunc) func;
    DoubleBuffer rowData = rows[0].getData();
    int size = rows[0].size();
    for (int i = 0; i < size; i++) {
      rowData.put(i, mapper.call(rowData.get(i)));
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows, Serialize func) {
    MapFunc mapper = (MapFunc) func;
    rows[0].tryToLockWrite();
    try {
      Long2DoubleOpenHashMap data1 = rows[0].getData();
      double defaultValue = data1.defaultReturnValue();
      data1.defaultReturnValue(mapper.call(defaultValue));

      ObjectIterator<Long2DoubleMap.Entry> iter = data1.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(mapper.call(entry.getDoubleValue()));
      }
    } finally {
      rows[0].unlockWrite();
    }
  }

}
