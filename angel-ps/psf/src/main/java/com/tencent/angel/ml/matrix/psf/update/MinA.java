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

import com.tencent.angel.ml.matrix.psf.update.enhance.VAUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

import java.nio.DoubleBuffer;

/**
 * `MinA` is find the minimum value of each element in `rowId` row and `other`
 */
public class MinA extends VAUpdateFunc {

  public MinA(int matrixId, int rowId, double[] other) {
    super(matrixId, rowId, other);
  }

  public MinA() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double[] other) {
    row.tryToLockWrite();
    try {
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, Math.min(data.get(i), other[i]));
      }
    } finally {
      row.unlockWrite();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow rows, double[] other) {
    throw new RuntimeException("MinA PSF can not support sparse type rows");
  }

}
