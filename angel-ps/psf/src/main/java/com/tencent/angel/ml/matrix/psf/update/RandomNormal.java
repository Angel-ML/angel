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
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

import java.nio.DoubleBuffer;
import java.util.Random;

/**
 * Generate a random array for `rowId`, each element belongs to normal distribution N(mean, stddev)
 */
public class RandomNormal extends MMUpdateFunc {

  public RandomNormal(int matrixId, int rowId, double mean, double stddev) {
    super(matrixId, new int[]{rowId}, new double[]{mean, stddev});
  }

  public RandomNormal() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] scalars) {
    Random rand = new Random(System.currentTimeMillis());

    try {
      rows[0].getLock().writeLock().lock();
      double mean = scalars[0];
      double stdDev = scalars[1];
      DoubleBuffer data = rows[0].getData();
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        data.put(i, stdDev * rand.nextGaussian() + mean);
      }
    } finally {
      rows[0].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows, double[] values) {
    throw new RuntimeException("RandomNormal PSF can not support sparse type rows");
  }

}
