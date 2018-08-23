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

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.enhance.VAUpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerLongDoubleRow;

/**
 * `MinA` is find the minimum value of each element in `rowId` row and `other`
 */
public class MinA extends VAUpdateFunc {

  //todo:implements
  public MinA(int matrixId, int rowId, Vector other) {
    super();
  }

  public MinA() {
    super();
  }

  @Override protected void doUpdate(ServerIntDoubleRow row, double[] other) {
    row.startWrite();
    try {
      double[] values = row.getValues();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        values[i] = Math.min(values[i], other[i]);
      }
    } finally {
      row.endWrite();
    }
  }

  @Override protected void doUpdate(ServerLongDoubleRow rows, double[] other) {
    throw new RuntimeException("MinA PSF can not support sparse type rows");
  }

}
