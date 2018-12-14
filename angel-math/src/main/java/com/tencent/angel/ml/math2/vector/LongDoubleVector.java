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
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.doubles.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

import java.lang.Math;

public class LongDoubleVector extends DoubleVector implements LongKeyVector, SimpleVector {
  private long dim;

  public long getDim() {
    return dim;
  }

  public long dim() {
    return (long) getDim();
  }

  public void setDim(long dim) {
    this.dim = dim;
  }

  public LongDoubleVector() {
    super();
  }

  public LongDoubleVector(int matrixId, int rowId, int clock, long dim,
    LongDoubleVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public LongDoubleVector(long dim, LongDoubleVectorStorage storage) {
    this(0, 0, 0, dim, storage);
  }

  public double get(long idx) {
    return ((LongDoubleVectorStorage) storage).get(idx);
  }

  public double[] get(long[] idxs) {
    return ((LongDoubleVectorStorage) storage).get(idxs);
  }

  public void set(long idx, double value) {
    ((LongDoubleVectorStorage) storage).set(idx, value);
  }

  public double max() {
    LongDoubleVectorStorage idstorage = (LongDoubleVectorStorage) storage;
    if (idstorage.size() == 0)
      return 0;
    double maxval = Double.MIN_VALUE;
    if (idstorage.isSparse()) {
      DoubleIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextDouble();
        if (val > maxval) {
          maxval = val;
        }
      }
    } else {
      for (double val : idstorage.getValues()) {
        if (val > maxval) {
          maxval = val;
        }
      }
    }
    return maxval;
  }

  public double min() {
    LongDoubleVectorStorage idstorage = (LongDoubleVectorStorage) storage;
    if (idstorage.size() == 0)
      return 0;
    double minval = Double.MAX_VALUE;
    if (idstorage.isSparse()) {
      DoubleIterator iter = idstorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextDouble();
        if (val < minval) {
          minval = val;
        }
      }
    } else {
      for (double val : idstorage.getValues()) {
        if (val < minval) {
          minval = val;
        }
      }
    }
    return minval;
  }

  public long argmax() {
    LongDoubleVectorStorage idstorage = (LongDoubleVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    double maxval = Double.MIN_VALUE;
    long maxidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        double val = entry.getDoubleValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      double[] val = idstorage.getValues();
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
    LongDoubleVectorStorage idstorage = (LongDoubleVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    double minval = Double.MAX_VALUE;
    long minidx = -1;
    if (idstorage.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        double val = entry.getDoubleValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      long[] indices = idstorage.getIndices();
      double[] val = idstorage.getValues();
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
    LongDoubleVectorStorage dstorage = (LongDoubleVectorStorage) storage;
    if (dstorage.size() == 0)
      return 0;
    double sumval = 0.0;
    double sumval2 = 0.0;
    if (dstorage.isSparse()) {
      DoubleIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextDouble();
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
    LongDoubleVectorStorage dstorage = (LongDoubleVectorStorage) storage;
    if (dstorage.size() == 0)
      return 0;
    double sumval = 0.0;
    if (dstorage.isSparse()) {
      DoubleIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        sumval += iter.nextDouble();
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
    return ((LongDoubleVectorStorage) storage).size();
  }

  public long numZeros() {
    LongDoubleVectorStorage dstorage = (LongDoubleVectorStorage) storage;
    if (dstorage.size() == 0)
      return (long) dim;
    long numZero = 0;
    if (dstorage.isSparse()) {
      DoubleIterator iter = dstorage.valueIterator();
      while (iter.hasNext()) {
        if (iter.nextDouble() != 0) {
          numZero += 1;
        }
      }
    } else {
      for (double val : dstorage.getValues()) {
        if (val != 0) {
          numZero += 1;
        }
      }
    }
    return (long) getDim() - numZero;
  }

  public LongDoubleVector clone() {
    return new LongDoubleVector(matrixId, rowId, clock, dim,
      ((LongDoubleVectorStorage) storage).clone());
  }

  @Override public LongDoubleVector copy() {
    return new LongDoubleVector(matrixId, rowId, clock, dim,
      ((LongDoubleVectorStorage) storage).copy());
  }

  @Override public LongDoubleVectorStorage getStorage() {
    return (LongDoubleVectorStorage) storage;
  }

  @Override public boolean hasKey(long idx) {
    return getStorage().hasKey(idx);
  }

  @Override public Vector filter(double threshold) {
    LongDoubleSparseVectorStorage newStorage = new LongDoubleSparseVectorStorage(size());

    if (storage.isDense()) {
      double[] values = ((LongDoubleVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        ((LongDoubleVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongDoubleVectorStorage) storage).getIndices();
      double[] values = ((LongDoubleVectorStorage) storage).getValues();

      long size = ((LongDoubleVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongDoubleVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      double[] values = ((LongDoubleVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        ((LongDoubleVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      long[] indices = ((LongDoubleVectorStorage) storage).getIndices();
      double[] values = ((LongDoubleVectorStorage) storage).getValues();

      long size = ((LongDoubleVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new LongDoubleVector(matrixId, rowId, clock, getDim(),
      (LongDoubleVectorStorage) storage);
  }

  @Override public Vector filterUp(double threshold) {
    LongDoubleSparseVectorStorage newStorage = new LongDoubleSparseVectorStorage(size());

    if (storage.isDense()) {
      double[] values = ((LongDoubleVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        ((LongDoubleVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (value >= threshold) {
          newStorage.set(entry.getLongKey(), value);
        }
      }
    } else {
      long[] indices = ((LongDoubleVectorStorage) storage).getIndices();
      double[] values = ((LongDoubleVectorStorage) storage).getValues();

      long size = ((LongDoubleVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new LongDoubleVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}