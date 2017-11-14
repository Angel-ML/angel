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

package com.tencent.angel.ml.matrix.psf.update.enhance.zip3;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.matrix.psf.update.enhance.MFUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

/**
 * It is a Zip3MapWithIndex function which applies `Zip3MapWithIndexFunc` to `fromId1`, `fromId2`
 * and `fromId3` row, and saves the result to `toId` row.
 */
public class Zip3MapWithIndex extends MFUpdateFunc {

  public Zip3MapWithIndex(
      int matrixId, int fromId1, int fromId2, int fromId3, int toId, Zip3MapWithIndexFunc func) {
    super(matrixId, new int[]{fromId1, fromId2, fromId3, toId}, func);
  }

  public Zip3MapWithIndex() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, Serialize func) {
    Zip3MapWithIndexFunc mapper = (Zip3MapWithIndexFunc) func;
    DoubleBuffer from1 = rows[0].getData();
    DoubleBuffer from2 = rows[1].getData();
    DoubleBuffer from3 = rows[2].getData();
    DoubleBuffer to = rows[3].getData();
    int startCol = (int)rows[0].getStartCol();
    int size = rows[0].size();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(startCol + i, from1.get(i), from2.get(i), from3.get(i)));
    }
  }
}
