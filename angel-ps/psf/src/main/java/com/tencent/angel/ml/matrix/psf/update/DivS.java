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

import java.nio.DoubleBuffer;

/**
 * `DivS` function will divide `fromId` by `value` and save to `toId`.
 * That is `toId` = `fromId` / `value`
 */
public class DivS extends MMUpdateFunc {

  public DivS(int matrixId, int fromId, int toId, double value) {
    super(matrixId, new int[]{fromId, toId}, new double[]{value});
  }

  public DivS() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] scalars) {
    DoubleBuffer from = rows[0].getData();
    DoubleBuffer to = rows[1].getData();
    double value = scalars[0];
    int size = rows[0].size();
    for (int i = 0; i < size; i++) {
      to.put(i, from.get(i) / value);
    }
  }

}
