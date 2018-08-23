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

import com.tencent.angel.ml.math2.storage.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

import java.lang.Math;

public class IntLongVector extends LongVector implements IntKeyVector, SimpleVector {
  private int dim;

  public int getDim() {
    return dim;
  }

  public long dim() {
    return (long) getDim();
  }

  public void setDim(int dim) {
    this.dim = dim;
  }

  public IntLongVector(int matrixId, int rowId, int clock, int dim, IntLongVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public IntLongVector(int dim, IntLongVectorStorage storage) {
    this(0, 0, 0, dim, storage);
  }

  public long get(int idx) {
    return ((IntLongVectorStorage) storage).get(idx);
  }

  public long[] get(int[] idxs) {
    return ((IntLongVectorStorage) storage).get(idxs);
  }

  public void set(int idx, long value) {
    ((IntLongVectorStorage) storage).set(idx, value);
  }

  public long max() {
    IntLongVectorStorage idstorage = (IntLongVectorStorage) storage;
    if (idstorage.size() == 0)
      return 0;
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
    IntLongVectorStorage idstorage = (IntLongVectorStorage) storage;
    if (idstorage.size() == 0)
      return 0;
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

  public int argmax() {
    IntLongVectorStorage idstorage = (IntLongVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    long maxval = Long.MIN_VALUE;
    int maxidx = -1;
    if (idstorage.isDense()) {
      long[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] > maxval) {
          maxval = val[idx];
          maxidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        long val = entry.getLongValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      long[] val = idstorage.getValues();
      int size = idstorage.size();
      for (int i = 0; i < size; i++) {
        int idx = indices[i];
        if (val[i] > maxval) {
          maxval = val[i];
          maxidx = idx;
        }
      }
    }
    return maxidx;
  }

  public int argmin() {
    IntLongVectorStorage idstorage = (IntLongVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    long minval = Long.MAX_VALUE;
    int minidx = -1;
    if (idstorage.isDense()) {
      long[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] < minval) {
          minval = val[idx];
          minidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        long val = entry.getLongValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      long[] val = idstorage.getValues();
      int size = idstorage.size();
      for (int i = 0; i < size; i++) {
        int idx = indices[i];
        if (val[i] < minval) {
          minval = val[i];
          minidx = idx;
        }
      }
    }
    return minidx;
  }

  public double std() {
    IntLongVectorStorage dstorage = (IntLongVectorStorage) storage;
    if (dstorage.size() == 0)
      return 0;
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
    IntLongVectorStorage dstorage = (IntLongVectorStorage) storage;
    if (dstorage.size() == 0)
      return 0;
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

  public int size() {
    return ((IntLongVectorStorage) storage).size();
  }

  public int numZeros() {
    IntLongVectorStorage dstorage = (IntLongVectorStorage) storage;
    if (dstorage.size() == 0)
      return (int) dim;
    int numZero = 0;
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
    return (int) getDim() - numZero;
  }

  public IntLongVector clone() {
    return new IntLongVector(matrixId, rowId, clock, dim, ((IntLongVectorStorage) storage).clone());
  }

  @Override public IntLongVector copy() {
    return new IntLongVector(matrixId, rowId, clock, dim, ((IntLongVectorStorage) storage).copy());
  }

  @Override public IntLongVectorStorage getStorage() {
    return (IntLongVectorStorage) storage;
  }

  @Override public boolean hasKey(int idx) {
    return getStorage().hasKey(idx);
  }

  @Override public Vector filter(double threshold) {
    IntLongSparseVectorStorage newStorage = new IntLongSparseVectorStorage(size());

    if (storage.isDense()) {
      long[] values = ((IntLongVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        long value = entry.getLongValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntLongVectorStorage) storage).getIndices();
      long[] values = ((IntLongVectorStorage) storage).getValues();

      int size = ((IntLongVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntLongVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      long[] values = ((IntLongVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        long value = entry.getLongValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      int[] indices = ((IntLongVectorStorage) storage).getIndices();
      long[] values = ((IntLongVectorStorage) storage).getValues();

      int size = ((IntLongVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new IntLongVector(matrixId, rowId, clock, getDim(), (IntLongVectorStorage) storage);
  }

  @Override public Vector filterUp(double threshold) {
    IntLongSparseVectorStorage newStorage = new IntLongSparseVectorStorage(size());

    if (storage.isDense()) {
      long[] values = ((IntLongVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        long value = entry.getLongValue();
        if (value >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntLongVectorStorage) storage).getIndices();
      long[] values = ((IntLongVectorStorage) storage).getValues();

      int size = ((IntLongVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntLongVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}
