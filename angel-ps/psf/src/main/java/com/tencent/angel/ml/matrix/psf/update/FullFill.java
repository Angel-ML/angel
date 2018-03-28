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

package com.tencent.angel.ml.matrix.psf.update;

import com.tencent.angel.ml.matrix.psf.update.enhance.FullUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

/**
 * `FullFill` a matrix with `value`
 */
public class FullFill extends FullUpdateFunc {

  public FullFill(int matrixId, double value) {
    super(matrixId, new double[]{value});
  }

  public FullFill() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] values) {
    for (ServerDenseDoubleRow row: rows) {
      double value = values[0];
      row.tryToLockWrite();
      try {
        DoubleBuffer rowData = row.getData();
        for (int j = 0; j < row.size(); j++) {
          rowData.put(j, value);
        }
      } finally {
        row.unlockWrite();
      }
    }
  }
}
