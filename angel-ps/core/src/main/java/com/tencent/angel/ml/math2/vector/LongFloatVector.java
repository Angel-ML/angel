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

import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

public class LongFloatVector extends FloatVector implements LongKeyVector, SimpleVector {
  private long dim;

  public LongFloatVector() {
    super();
  }

  public LongFloatVector(int matrixId, int rowId, int clock, long dim, LongFloatVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public LongFloatVector(long dim, LongFloatVectorStorage storage) {
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

  public float get(long idx) {
    return ((LongFloatVectorStorage) storage).get(idx);
  }

  public float[] get(long[] idxs) {
    return ((LongFloatVectorStorage) storage).get(idxs);
  }

  public void set(long idx, float value) {
    ((LongFloatVectorStorage) storage).set(idx, value);
  }

  public float max() {
    LongFloatVectorStorage idstorage = (LongFloatVectorStorage) storage;
    if (idstorage.size() == 0) return 0;
    float maxval = Float.MIN_VALUE;
    if (idstorage.isSparse()) {
      FloatIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        float val = iter.nextFloat();
        if (val > maxval) {
          maxval = val;
        }
      }
    } else {
      for (float val : idstorage.getValues()) {
        if (val > maxval) {
          maxval = val;
        }
      }
    }
    return maxval;
  }

  public float min() {
    LongFloatVectorStorage idstorage = (LongFloatVectorStorage) storage;
    if (idstorage.size() == 0) return 0;
    float minval = Float.MAX_VALUE;
    if (idstorage.isSparse()) {
      FloatIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        float val = iter.nextFloat();
        if (val < minval) {
          minval = val;
        }
      }
    } else {
      for (float val : idstorage.getValues()) {
        if (val < minval) {
          minval = val;
        }
      }
    }
    return minval;
  }

  public long argmax() {
    LongFloatVectorStorage idstorage = (LongFloatVectorStorage) storage;
    if (idstorage.size() == 0) return -1;
    float maxval = Float.MIN_VALUE;
    long maxidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        float val = entry.getFloatValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      float[] val = idstorage.getValues();
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
    LongFloatVectorStorage idstorage = (LongFloatVectorStorage) storage;
    if (idstorage.size() == 0) return -1;
    float minval = Float.MAX_VALUE;
    long minidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        float val = entry.getFloatValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      float[] val = idstorage.getValues();
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
    LongFloatVectorStorage dstorage = (LongFloatVectorStorage) storage;
    if (dstorage.size() == 0) return 0;
    double sumval = 0.0;
    double sumval2 = 0.0;
    if (dstorage.isSparse()) {
      FloatIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextFloat();
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
    LongFloatVectorStorage dstorage = (LongFloatVectorStorage) storage;
    if (dstorage.size() == 0) return 0;
    double sumval = 0.0;
    if (dstorage.isSparse()) {
      FloatIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        sumval += iter.nextFloat();
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
    return ((LongFloatVectorStorage) storage).size();
  }

  public long numZeros() {
    LongFloatVectorStorage dstorage = (LongFloatVectorStorage) storage;
    if (dstorage.size() == 0) return (long) dim;
    long numZero = 0;
    if (dstorage.isSparse()) {
      FloatIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        if (iter.nextFloat() != 0) {
          numZero += 1;
        }
      }
    } else {
      for (float val : dstorage.getValues()) {
        if (val != 0) {
          numZero += 1;
        }
      }
    }
    return (long) getDim() - numZero;
  }

  public LongFloatVector clone() {
    return new LongFloatVector(matrixId, rowId, clock, dim,
        ((LongFloatVectorStorage) storage).clone());
  }

  @Override
  public LongFloatVector copy() {
    return new LongFloatVector(matrixId, rowId, clock, dim,
        ((LongFloatVectorStorage) storage).copy());
  }

  @Override
  public LongFloatVector emptyLike() {
    if (storage.isSparse()) {
      return new LongFloatVector(matrixId, rowId, clock, dim,
          ((LongFloatVectorStorage) storage).emptySparse());
    } else {
      return new LongFloatVector(matrixId, rowId, clock, dim,
          ((LongFloatVectorStorage) storage).emptySorted());
    }
  }

  @Override
  public LongFloatVectorStorage getStorage() {
    return (LongFloatVectorStorage) storage;
  }

  @Override
  public boolean hasKey(long idx) {
    return getStorage().hasKey(idx);
  }

  @Override
  public Vector filter(double threshold) {
    LongFloatSparseVectorStorage newStorage = new LongFloatSparseVectorStorage(size());

    if (storage.isDense()) {
      float[] values = ((LongFloatVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = ((LongFloatVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongFloatVectorStorage) storage).getIndices();
      float[] values = ((LongFloatVectorStorage) storage).getValues();

      long size = ((LongFloatVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongFloatVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override
  public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      float[] values = ((LongFloatVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = ((LongFloatVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      long[] indices = ((LongFloatVectorStorage) storage).getIndices();
      float[] values = ((LongFloatVectorStorage) storage).getValues();

      long size = ((LongFloatVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new LongFloatVector(matrixId, rowId, clock, getDim(), (LongFloatVectorStorage) storage);
  }

  @Override
  public Vector filterUp(double threshold) {
    LongFloatSparseVectorStorage newStorage = new LongFloatSparseVectorStorage(size());

    if (storage.isDense()) {
      float[] values = ((LongFloatVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = ((LongFloatVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (value >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongFloatVectorStorage) storage).getIndices();
      float[] values = ((LongFloatVectorStorage) storage).getValues();

      long size = ((LongFloatVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongFloatVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}
