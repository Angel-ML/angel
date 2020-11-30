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

import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

public class LongIntVector extends IntVector implements LongKeyVector, SimpleVector {
  private long dim;

  public LongIntVector() {
    super();
  }

  public LongIntVector(int matrixId, int rowId, int clock, long dim, LongIntVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public LongIntVector(long dim, LongIntVectorStorage storage) {
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

  public int get(long idx) {
    return ((LongIntVectorStorage) storage).get(idx);
  }

  public int[] get(long[] idxs) {
    return ((LongIntVectorStorage) storage).get(idxs);
  }

  public void set(long idx, int value) {
    ((LongIntVectorStorage) storage).set(idx, value);
  }

  public int max() {
    LongIntVectorStorage idstorage = (LongIntVectorStorage) storage;
    if (idstorage.size() == 0) return 0;
    int maxval = Integer.MIN_VALUE;
    if (idstorage.isSparse()) {
      IntIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        int val = iter.nextInt();
        if (val > maxval) {
          maxval = val;
        }
      }
    } else {
      for (int val : idstorage.getValues()) {
        if (val > maxval) {
          maxval = val;
        }
      }
    }
    return maxval;
  }

  public int min() {
    LongIntVectorStorage idstorage = (LongIntVectorStorage) storage;
    if (idstorage.size() == 0) return 0;
    int minval = Integer.MAX_VALUE;
    if (idstorage.isSparse()) {
      IntIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        int val = iter.nextInt();
        if (val < minval) {
          minval = val;
        }
      }
    } else {
      for (int val : idstorage.getValues()) {
        if (val < minval) {
          minval = val;
        }
      }
    }
    return minval;
  }

  public long argmax() {
    LongIntVectorStorage idstorage = (LongIntVectorStorage) storage;
    if (idstorage.size() == 0) return -1;
    int maxval = Integer.MIN_VALUE;
    long maxidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        int val = entry.getIntValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      int[] val = idstorage.getValues();
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
    LongIntVectorStorage idstorage = (LongIntVectorStorage) storage;
    if (idstorage.size() == 0) return -1;
    int minval = Integer.MAX_VALUE;
    long minidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        int val = entry.getIntValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      int[] val = idstorage.getValues();
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
    LongIntVectorStorage dstorage = (LongIntVectorStorage) storage;
    if (dstorage.size() == 0) return 0;
    double sumval = 0.0;
    double sumval2 = 0.0;
    if (dstorage.isSparse()) {
      IntIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextInt();
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
    LongIntVectorStorage dstorage = (LongIntVectorStorage) storage;
    if (dstorage.size() == 0) return 0;
    double sumval = 0.0;
    if (dstorage.isSparse()) {
      IntIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        sumval += iter.nextInt();
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
    return ((LongIntVectorStorage) storage).size();
  }

  public long numZeros() {
    LongIntVectorStorage dstorage = (LongIntVectorStorage) storage;
    if (dstorage.size() == 0) return (long) dim;
    long numZero = 0;
    if (dstorage.isSparse()) {
      IntIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        if (iter.nextInt() != 0) {
          numZero += 1;
        }
      }
    } else {
      for (int val : dstorage.getValues()) {
        if (val != 0) {
          numZero += 1;
        }
      }
    }
    return (long) getDim() - numZero;
  }

  public LongIntVector clone() {
    return new LongIntVector(matrixId, rowId, clock, dim,
        ((LongIntVectorStorage) storage).clone());
  }

  @Override
  public LongIntVector copy() {
    return new LongIntVector(matrixId, rowId, clock, dim,
        ((LongIntVectorStorage) storage).copy());
  }

  @Override
  public LongIntVector emptyLike() {
    if (storage.isSparse()) {
      return new LongIntVector(matrixId, rowId, clock, dim,
          ((LongIntVectorStorage) storage).emptySparse());
    } else {
      return new LongIntVector(matrixId, rowId, clock, dim,
          ((LongIntVectorStorage) storage).emptySorted());
    }
  }

  @Override
  public LongIntVectorStorage getStorage() {
    return (LongIntVectorStorage) storage;
  }

  @Override
  public boolean hasKey(long idx) {
    return getStorage().hasKey(idx);
  }

  @Override
  public Vector filter(double threshold) {
    LongIntSparseVectorStorage newStorage = new LongIntSparseVectorStorage(size());

    if (storage.isDense()) {
      int[] values = ((LongIntVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = ((LongIntVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        int value = entry.getIntValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongIntVectorStorage) storage).getIndices();
      int[] values = ((LongIntVectorStorage) storage).getValues();

      long size = ((LongIntVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongIntVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override
  public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      int[] values = ((LongIntVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = ((LongIntVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        int value = entry.getIntValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      long[] indices = ((LongIntVectorStorage) storage).getIndices();
      int[] values = ((LongIntVectorStorage) storage).getValues();

      long size = ((LongIntVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new LongIntVector(matrixId, rowId, clock, getDim(), (LongIntVectorStorage) storage);
  }

  @Override
  public Vector filterUp(double threshold) {
    LongIntSparseVectorStorage newStorage = new LongIntSparseVectorStorage(size());

    if (storage.isDense()) {
      int[] values = ((LongIntVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = ((LongIntVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        int value = entry.getIntValue();
        if (value >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongIntVectorStorage) storage).getIndices();
      int[] values = ((LongIntVectorStorage) storage).getValues();

      long size = ((LongIntVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongIntVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}
