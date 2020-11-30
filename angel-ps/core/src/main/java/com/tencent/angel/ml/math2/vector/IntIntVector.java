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

import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

public class IntIntVector extends IntVector implements IntKeyVector, SimpleVector {
  private int dim;

  public IntIntVector() {
    super();
  }

  public IntIntVector(int matrixId, int rowId, int clock, int dim, IntIntVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public IntIntVector(int dim, IntIntVectorStorage storage) {
    this(0, 0, 0, dim, storage);
  }

  public int getDim() {
    return dim;
  }

  public void setDim(int dim) {
    this.dim = dim;
  }

  public long dim() {
    return (long) getDim();
  }

  public int get(int idx) {
    return ((IntIntVectorStorage) storage).get(idx);
  }

  public int[] get(int[] idxs) {
    return ((IntIntVectorStorage) storage).get(idxs);
  }

  public void set(int idx, int value) {
    ((IntIntVectorStorage) storage).set(idx, value);
  }

  public int max() {
    IntIntVectorStorage idstorage = (IntIntVectorStorage) storage;
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
    IntIntVectorStorage idstorage = (IntIntVectorStorage) storage;
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

  public int argmax() {
    IntIntVectorStorage idstorage = (IntIntVectorStorage) storage;
    if (idstorage.size() == 0) return -1;
    int maxval = Integer.MIN_VALUE;
    int maxidx = -1;
    if (idstorage.isDense()) {
      int[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] > maxval) {
          maxval = val[idx];
          maxidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        int val = entry.getIntValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      int[] val = idstorage.getValues();
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
    IntIntVectorStorage idstorage = (IntIntVectorStorage) storage;
    if (idstorage.size() == 0) return -1;
    int minval = Integer.MAX_VALUE;
    int minidx = -1;
    if (idstorage.isDense()) {
      int[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] < minval) {
          minval = val[idx];
          minidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        int val = entry.getIntValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      int[] val = idstorage.getValues();
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
    IntIntVectorStorage dstorage = (IntIntVectorStorage) storage;
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
    IntIntVectorStorage dstorage = (IntIntVectorStorage) storage;
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

  public int size() {
    return ((IntIntVectorStorage) storage).size();
  }

  public int numZeros() {
    IntIntVectorStorage dstorage = (IntIntVectorStorage) storage;
    if (dstorage.size() == 0) return (int) dim;
    int numZero = 0;
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
    return (int) getDim() - numZero;
  }

  public IntIntVector clone() {
    return new IntIntVector(matrixId, rowId, clock, dim,
        ((IntIntVectorStorage) storage).clone());
  }

  @Override
  public IntIntVector copy() {
    return new IntIntVector(matrixId, rowId, clock, dim,
        ((IntIntVectorStorage) storage).copy());
  }

  @Override
  public IntIntVector emptyLike() {
    if (storage.isDense()) {
      return new IntIntVector(matrixId, rowId, clock, dim,
          ((IntIntVectorStorage) storage).emptyDense());
    } else if (storage.isSparse()) {
      return new IntIntVector(matrixId, rowId, clock, dim,
          ((IntIntVectorStorage) storage).emptySparse());
    } else {
      return new IntIntVector(matrixId, rowId, clock, dim,
          ((IntIntVectorStorage) storage).emptySorted());
    }
  }

  @Override
  public IntIntVectorStorage getStorage() {
    return (IntIntVectorStorage) storage;
  }

  @Override
  public boolean hasKey(int idx) {
    return getStorage().hasKey(idx);
  }

  @Override
  public Vector filter(double threshold) {
    IntIntSparseVectorStorage newStorage = new IntIntSparseVectorStorage(size());

    if (storage.isDense()) {
      int[] values = ((IntIntVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int value = entry.getIntValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntIntVectorStorage) storage).getIndices();
      int[] values = ((IntIntVectorStorage) storage).getValues();

      int size = ((IntIntVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntIntVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override
  public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      int[] values = ((IntIntVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int value = entry.getIntValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      int[] indices = ((IntIntVectorStorage) storage).getIndices();
      int[] values = ((IntIntVectorStorage) storage).getValues();

      int size = ((IntIntVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new IntIntVector(matrixId, rowId, clock, getDim(), (IntIntVectorStorage) storage);
  }

  @Override
  public Vector filterUp(double threshold) {
    IntIntSparseVectorStorage newStorage = new IntIntSparseVectorStorage(size());

    if (storage.isDense()) {
      int[] values = ((IntIntVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int value = entry.getIntValue();
        if (value >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntIntVectorStorage) storage).getIndices();
      int[] values = ((IntIntVectorStorage) storage).getValues();

      int size = ((IntIntVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntIntVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}
