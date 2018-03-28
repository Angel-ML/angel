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
 * Init a random matrix, whose value is a random value between 0.0 and 1.0.
 */
public class Random extends FullUpdateFunc {

  public Random(int matrixId) {
    super(matrixId, new double[]{});
  }

  public Random() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] value) {
    for (ServerDenseDoubleRow row: rows) {
      row.tryToLockWrite();
      try {
        DoubleBuffer rowData = row.getData();
        java.util.Random rand = new java.util.Random(row.getRowId());
        for (int j = 0; j < row.size(); j++) {
          rowData.put(j, rand.nextDouble());
        }
      } finally {
        row.unlockWrite();
      }
    }
  }

}
