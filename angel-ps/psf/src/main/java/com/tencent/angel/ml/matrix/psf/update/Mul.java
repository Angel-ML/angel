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
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.nio.DoubleBuffer;
import java.util.Map;

/**
 * `Mul` function will multiply `fromId1` and `fromId2` and saves to `toId`.
 * That is `toId` = `fromId1` * `fromId2`
 */
public class Mul extends MUpdateFunc {

  public Mul(int matrixId, int fromId1, int fromId2, int toId) {
    super(matrixId, new int[]{fromId1, fromId2, toId});
  }

  public Mul() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows) {
    rows[2].tryToLockWrite();
    try {
      DoubleBuffer from1 = rows[0].getData();
      DoubleBuffer from2 = rows[1].getData();
      DoubleBuffer to = rows[2].getData();
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        to.put(i, from1.get(i) * from2.get(i));
      }
    } finally {
      rows[2].unlockWrite();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows) {
    Long2DoubleOpenHashMap from1 = rows[0].getIndex2ValueMap();
    Long2DoubleOpenHashMap from2 = rows[1].getIndex2ValueMap();

    LongOpenHashSet keySet = new LongOpenHashSet(from1.keySet());
    keySet.addAll(from2.keySet());

    Long2DoubleOpenHashMap to = new Long2DoubleOpenHashMap(keySet.size());

    LongIterator iter = keySet.iterator();
    while (iter.hasNext()) {
      long key = iter.nextLong();
      to.put(key, from1.get(key) * from2.get(key));
    }
    double default1 = from1.defaultReturnValue();
    double default2 = from2.defaultReturnValue();
    to.defaultReturnValue(default1 * default2);

    rows[2].setIndex2ValueMap(to);
  }
}
