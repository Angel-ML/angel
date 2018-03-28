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
 * Convert a zero-matrix to a diagonal matrix
 */
public class Diag extends FullUpdateFunc {

  public Diag(int matrixId, double[] values) {
    super(matrixId, values);
  }

  public Diag() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] values) {
    for (ServerDenseDoubleRow row : rows) {
      int rowId = row.getRowId();
      if (rowId >= row.getStartCol() && rowId < row.getEndCol()) {
        row.tryToLockWrite();
        try {
          DoubleBuffer rowData = row.getData();
          rowData.put(rowId - (int)row.getStartCol(), values[rowId]);
        } finally {
          row.unlockWrite();
        }
      }
    }
  }
}
