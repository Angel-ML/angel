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


package com.tencent.angel.ml.math2.vector;

import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

public class LongLongVector extends LongVector implements LongKeyVector, SimpleVector {

  private long dim;

  public LongLongVector() {
    super();
  }

  public LongLongVector(int matrixId, int rowId, int clock, long dim,
      LongLongVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public LongLongVector(long dim, LongLongVectorStorage storage) {
    this(0, 0, 0, dim, storage);
  }

  public long getDim() {
    return dim;
  }

  public void setDim(long dim) {
    this.dim = dim;
  }

  public long dim() {
    return (long) getDim();
  }

  public long get(long idx) {
    return ((LongLongVectorStorage) storage).get(idx);
  }

  public long[] get(long[] idxs) {
    return ((LongLongVectorStorage) storage).get(idxs);
  }

  public void set(long idx, long value) {
    ((LongLongVectorStorage) storage).set(idx, value);
  }

  public long max() {
    LongLongVectorStorage idstorage = (LongLongVectorStorage) storage;
    if (idstorage.size() == 0) {
      return 0;
    }
    long maxval = Long.MIN_VALUE;
    if (idstorage.isSparse()) {
      LongIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        long val = iter.nextLong();
        if (val > maxval) {
          maxval = val;
        }
      }
    } else {
      for (long val : idstorage.getValues()) {
        if (val > maxval) {
          maxval = val;
        }
      }
    }
    return maxval;
  }

  public long min() {
    LongLongVectorStorage idstorage = (LongLongVectorStorage) storage;
    if (idstorage.size() == 0) {
      return 0;
    }
    long minval = Long.MAX_VALUE;
    if (idstorage.isSparse()) {
      LongIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        long val = iter.nextLong();
        if (val < minval) {
          minval = val;
        }
      }
    } else {
      for (long val : idstorage.getValues()) {
        if (val < minval) {
          minval = val;
        }
      }
    }
    return minval;
  }

  public long argmax() {
    LongLongVectorStorage idstorage = (LongLongVectorStorage) storage;
    if (idstorage.size() == 0) {
      return -1;
    }
    long maxval = Long.MIN_VALUE;
    long maxidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        long val = entry.getLongValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      long[] val = idstorage.getValues();
      long size = idstorage.size();
      for (int i = 0; i < size; i++) {
        long idx = indices[i];
        if (val[i] > maxval) {
          maxval = val[i];
          maxidx = idx;
        }
      }
    }
    return maxidx;
  }

  public long argmin() {
    LongLongVectorStorage idstorage = (LongLongVectorStorage) storage;
    if (idstorage.size() == 0) {
      return -1;
    }
    long minval = Long.MAX_VALUE;
    long minidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        long val = entry.getLongValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      long[] val = idstorage.getValues();
      long size = idstorage.size();
      for (int i = 0; i < size; i++) {
        long idx = indices[i];
        if (val[i] < minval) {
          minval = val[i];
          minidx = idx;
        }
      }
    }
    return minidx;
  }

  public double std() {
    LongLongVectorStorage dstorage = (LongLongVectorStorage) storage;
    if (dstorage.size() == 0) {
      return 0;
    }
    double sumval = 0.0;
    double sumval2 = 0.0;
    if (dstorage.isSparse()) {
      LongIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextLong();
        sumval += val;
        sumval2 += val * val;
      }
    } else {
      for (double val : dstorage.getValues()) {
        sumval += val;
        sumval2 += val * val;
      }
    }
    sumval /= getDim();
    sumval2 /= getDim();
    return Math.sqrt(sumval2 - sumval * sumval);
  }

  public double average() {
    LongLongVectorStorage dstorage = (LongLongVectorStorage) storage;
    if (dstorage.size() == 0) {
      return 0;
    }
    double sumval = 0.0;
    if (dstorage.isSparse()) {
      LongIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        sumval += iter.nextLong();
      }
    } else {
      for (double val : dstorage.getValues()) {
        sumval += val;
      }
    }

    sumval /= getDim();
    return sumval;
  }

  public long size() {
    return ((LongLongVectorStorage) storage).size();
  }

  public long numZeros() {
    LongLongVectorStorage dstorage = (LongLongVectorStorage) storage;
    if (dstorage.size() == 0) {
      return (long) dim;
    }
    long numZero = 0;
    if (dstorage.isSparse()) {
      LongIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        if (iter.nextLong() != 0) {
          numZero += 1;
        }
      }
    } else {
      for (long val : dstorage.getValues()) {
        if (val != 0) {
          numZero += 1;
        }
      }
    }
    return (long) getDim() - numZero;
  }

  public LongLongVector clone() {
    return new LongLongVector(matrixId, rowId, clock, dim,
        ((LongLongVectorStorage) storage).clone());
  }

  @Override
  public LongLongVector copy() {
    return new LongLongVector(matrixId, rowId, clock, dim,
        ((LongLongVectorStorage) storage).copy());
  }

  @Override
  public LongLongVectorStorage getStorage() {
    return (LongLongVectorStorage) storage;
  }

  @Override
  public boolean hasKey(long idx) {
    return getStorage().hasKey(idx);
  }

  @Override
  public Vector filter(double threshold) {
    LongLongSparseVectorStorage newStorage = new LongLongSparseVectorStorage(size());

    if (storage.isDense()) {
      long[] values = ((LongLongVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = ((LongLongVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long value = entry.getLongValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongLongVectorStorage) storage).getIndices();
      long[] values = ((LongLongVectorStorage) storage).getValues();

      long size = ((LongLongVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongLongVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override
  public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      long[] values = ((LongLongVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = ((LongLongVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long value = entry.getLongValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      long[] indices = ((LongLongVectorStorage) storage).getIndices();
      long[] values = ((LongLongVectorStorage) storage).getValues();

      long size = ((LongLongVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new LongLongVector(matrixId, rowId, clock, getDim(), (LongLongVectorStorage) storage);
  }

  @Override
  public Vector filterUp(double threshold) {
    LongLongSparseVectorStorage newStorage = new LongLongSparseVectorStorage(size());

    if (storage.isDense()) {
      long[] values = ((LongLongVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = ((LongLongVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long value = entry.getLongValue();
        if (value >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongLongVectorStorage) storage).getIndices();
      long[] values = ((LongLongVectorStorage) storage).getValues();

      long size = ((LongLongVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongLongVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}