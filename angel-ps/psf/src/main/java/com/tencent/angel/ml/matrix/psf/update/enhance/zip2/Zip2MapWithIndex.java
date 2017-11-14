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

package com.tencent.angel.ml.matrix.psf.update.enhance.zip2;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.matrix.psf.update.enhance.MFUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

/**
 * It is a Zip2MapWithIndex function which applies `Zip2MapWithIndexFunc` to `fromId1` and
 * `fromId2` row and saves the result to `toId` row.
 */
public class Zip2MapWithIndex extends MFUpdateFunc {

  public Zip2MapWithIndex(
      int matrixId, int fromId1, int fromId2, int toId, Zip2MapWithIndexFunc func) {
    super(matrixId, new int[] {fromId1, fromId2, toId}, func);
  }

  public Zip2MapWithIndex() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, Serialize func) {

    Zip2MapWithIndexFunc mapper = (Zip2MapWithIndexFunc) func;
    DoubleBuffer from1 = rows[0].getData();
    DoubleBuffer from2 = rows[1].getData();
    DoubleBuffer to = rows[2].getData();
    int size = rows[0].size();
    int startCol = (int)rows[0].getStartCol();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(startCol + i, from1.get(i), from2.get(i)));
    }
  }
}
