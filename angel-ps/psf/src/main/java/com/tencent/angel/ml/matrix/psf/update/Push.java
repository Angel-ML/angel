/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
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
 * `Push` the `values` to `rowId` row
 */
public class Push extends VAUpdateFunc {

  public Push(int matrixId, int rowId, double[] values) {
    super(matrixId, rowId, values);
  }

  public Push() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double[] values) {
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      data.put(i, values[i]);
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow row, double[] values) {
    throw new RuntimeException("Push PSF can not support sparse type rows");
  }

}
