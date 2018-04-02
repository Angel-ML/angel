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

/**
 * `Fill` the `rowId` row with `value`
 */
public class Fill extends MMUpdateFunc {

  public Fill(int matrixId, int rowId, double value) {
    super(matrixId, new int[]{rowId}, new double[]{value});
  }

  public Fill() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] values) {
    rows[0].tryToLockWrite();
    try {
      DoubleBuffer data = rows[0].getData();
      double value = values[0];
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        data.put(i, value);
      }
    } finally {
      rows[0].unlockWrite();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows, double[] values) {
    rows[0].tryToLockWrite();
    try {
      Long2DoubleOpenHashMap data = rows[0].getIndex2ValueMap();
      data.clear();
      data.defaultReturnValue(values[0]);
    } finally {
      rows[0].unlockWrite();
    }
  }
}
