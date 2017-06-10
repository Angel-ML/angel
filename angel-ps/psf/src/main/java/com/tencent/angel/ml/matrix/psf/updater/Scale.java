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
 * `Scale` function do scale to `rowId` row.
 * That is `rowId` *= `scaleFactor`
 */
public class Scale extends MMUpdaterFunc {

  public Scale(int matrixId, int rowId, double scaleFactor) {
    super(matrixId, new int[]{rowId}, new double[]{scaleFactor});
  }

  public Scale() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] values) {
    DoubleBuffer data = rows[0].getData();
    double scaleFactor = values[0];
    int size = rows[0].size();
    for (int i = 0; i < size; i++) {
      data.put(i, data.get(i) * scaleFactor);
    }
  }

}
