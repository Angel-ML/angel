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

package com.tencent.angel.ml.matrix.psf.updater.map;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.matrix.psf.updater.base.MFUpdateFunc;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

/**
 * It is a Map function which applies `MapFunc` to `fromId` row and saves the result to `toId` row
 */
public class Map extends MFUpdateFunc {

  public Map(int matrixId, int fromId, int toId, MapFunc func) {
    super(matrixId, new int[]{fromId, toId}, func);
  }

  public Map() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, Serialize func) {
    MapFunc mapper = (MapFunc) func;
    DoubleBuffer from = rows[0].getData();
    DoubleBuffer to = rows[1].getData();
    int size = rows[0].size();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(from.get(i)));
    }
  }
}
