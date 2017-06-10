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

package com.tencent.angel.ml.matrix.psf.updater;

import com.tencent.angel.ml.matrix.psf.updater.base.MMUpdaterFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

/**
 * `AddS` function will add a `value` to `fromId` and save result to `toId`.
 * That is `toId` = `fromId` + `value`
 */
public class AddS extends MMUpdaterFunc {

  public AddS(int matrixId, int fromId, int toId, double scalar) {
    super(matrixId, new int[] {fromId, toId}, new double[]{scalar});
  }

  public AddS() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] scalars) {
    DoubleBuffer from = rows[0].getData();
    DoubleBuffer to = rows[1].getData();
    int size = rows[0].size();
    for (int i = 0; i < size; i++) {
      to.put(i, from.get(i) + scalars[0]);
    }
  }

}
