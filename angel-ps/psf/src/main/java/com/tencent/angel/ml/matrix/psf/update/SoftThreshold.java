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
import com.tencent.angel.ps.impl.matrix.*;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;

/**
 * `AddS` function will add a `value` to `fromId` and save result to `toId`.
 * That is `toId` = `fromId` + `value`
 */
public class SoftThreshold extends MMUpdateFunc {

  public SoftThreshold(int matrixId, int rowId, double threshold) {
    super(matrixId, new int[]{rowId}, new double[]{threshold});
  }

  public SoftThreshold() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] thresholds) {
    try {
      rows[0].getLock().writeLock().lock();
      DoubleBuffer data = rows[0].getData();
      double threshold = thresholds[0];
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        double value = data.get(i);
        if (value > threshold) {
          data.put(i, value - threshold);
        } else if (value < - threshold ) {
          data.put(i, value + threshold);
        } else {
          data.put(i, 0.0);
        }
      }
    } finally {
      rows[0].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleRow[] rows, double[] thresholds) {
    try {
      rows[0].getLock().writeLock().lock();
      Int2DoubleOpenHashMap data = rows[0].getData();
      double threshold = thresholds[0];
      ObjectIterator<Int2DoubleMap.Entry> iter = data.int2DoubleEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (value > threshold) {
          entry.setValue(value - threshold);
        } else if (value < - threshold ) {
          entry.setValue(value + threshold);
        } else {
          iter.remove();
        }
      }
    } finally {
      rows[0].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows, double[] thresholds) {
    try {
      rows[0].getLock().writeLock().lock();
      Long2DoubleOpenHashMap data = rows[0].getData();
      double threshold = thresholds[0];
      ObjectIterator<Long2DoubleMap.Entry> iter = data.long2DoubleEntrySet().fastIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (value > threshold) {
          entry.setValue(value - threshold);
        } else if (value < - threshold ) {
          entry.setValue(value + threshold);
        } else {
          iter.remove();
        }
      }
    } finally {
      rows[0].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerDenseFloatRow[] rows, double[] thresholds) {
    try {
      rows[0].getLock().writeLock().lock();
      FloatBuffer data = rows[0].getData();
      float threshold = Double.valueOf(thresholds[0]).floatValue();
      int size = rows[0].size();
      for (int i = 0; i < size; i++) {
        float value = data.get(i);
        if (value > threshold) {
          data.put(i, value - threshold);
        } else if (value < - threshold ) {
          data.put(i, value + threshold);
        } else {
          data.put(i, 0.0f);
        }
      }
    } finally {
      rows[0].getLock().writeLock().unlock();
    }
  }

  @Override
  protected void doUpdate(ServerSparseFloatRow[] rows, double[] thresholds) {
    try {
      rows[0].getLock().writeLock().lock();
      Int2FloatOpenHashMap data = rows[0].getData();
      float threshold = Double.valueOf(thresholds[0]).floatValue();
      ObjectIterator<Int2FloatMap.Entry> iter = data.int2FloatEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (value > threshold) {
          entry.setValue(value - threshold);
        } else if (value < - threshold ) {
          entry.setValue(value + threshold);
        } else {
          iter.remove();
        }
      }
    } finally {
      rows[0].getLock().writeLock().unlock();
    }
  }
}
