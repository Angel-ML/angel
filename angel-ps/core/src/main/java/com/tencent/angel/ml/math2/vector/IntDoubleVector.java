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
import it.unimi.dsi.fastutil.doubles.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

import java.lang.Math;

public class IntDoubleVector extends DoubleVector implements IntKeyVector, SimpleVector {
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

  public IntDoubleVector(int matrixId, int rowId, int clock, int dim,
    IntDoubleVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public IntDoubleVector(int dim, IntDoubleVectorStorage storage) {
    this(0, 0, 0, dim, storage);
  }

  public double get(int idx) {
    return ((IntDoubleVectorStorage) storage).get(idx);
  }

  public double[] get(int[] idxs) {
    return ((IntDoubleVectorStorage) storage).get(idxs);
  }

  public void set(int idx, double value) {
    ((IntDoubleVectorStorage) storage).set(idx, value);
  }

  public double max() {
    IntDoubleVectorStorage idstorage = (IntDoubleVectorStorage) storage;
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
    IntDoubleVectorStorage idstorage = (IntDoubleVectorStorage) storage;
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

  public int argmax() {
    IntDoubleVectorStorage idstorage = (IntDoubleVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    double maxval = Double.MIN_VALUE;
    int maxidx = -1;
    if (idstorage.isDense()) {
      double[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] > maxval) {
          maxval = val[idx];
          maxidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        double val = entry.getDoubleValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      double[] val = idstorage.getValues();
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
    IntDoubleVectorStorage idstorage = (IntDoubleVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    double minval = Double.MAX_VALUE;
    int minidx = -1;
    if (idstorage.isDense()) {
      double[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] < minval) {
          minval = val[idx];
          minidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        double val = entry.getDoubleValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      double[] val = idstorage.getValues();
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
    IntDoubleVectorStorage dstorage = (IntDoubleVectorStorage) storage;
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
    IntDoubleVectorStorage dstorage = (IntDoubleVectorStorage) storage;
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

  public int size() {
    return ((IntDoubleVectorStorage) storage).size();
  }

  public int numZeros() {
    IntDoubleVectorStorage dstorage = (IntDoubleVectorStorage) storage;
    if (dstorage.size() == 0)
      return (int) dim;
    int numZero = 0;
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
    return (int) getDim() - numZero;
  }

  public IntDoubleVector clone() {
    return new IntDoubleVector(matrixId, rowId, clock, dim,
      ((IntDoubleVectorStorage) storage).clone());
  }

  @Override public IntDoubleVector copy() {
    return new IntDoubleVector(matrixId, rowId, clock, dim,
      ((IntDoubleVectorStorage) storage).copy());
  }

  @Override public IntDoubleVectorStorage getStorage() {
    return (IntDoubleVectorStorage) storage;
  }

  @Override public boolean hasKey(int idx) {
    return getStorage().hasKey(idx);
  }

  @Override public Vector filter(double threshold) {
    IntDoubleSparseVectorStorage newStorage = new IntDoubleSparseVectorStorage(size());

    if (storage.isDense()) {
      double[] values = ((IntDoubleVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = ((IntDoubleVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntDoubleVectorStorage) storage).getIndices();
      double[] values = ((IntDoubleVectorStorage) storage).getValues();

      int size = ((IntDoubleVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntDoubleVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      double[] values = ((IntDoubleVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = ((IntDoubleVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      int[] indices = ((IntDoubleVectorStorage) storage).getIndices();
      double[] values = ((IntDoubleVectorStorage) storage).getValues();

      int size = ((IntDoubleVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new IntDoubleVector(matrixId, rowId, clock, getDim(), (IntDoubleVectorStorage) storage);
  }

  @Override public Vector filterUp(double threshold) {
    IntDoubleSparseVectorStorage newStorage = new IntDoubleSparseVectorStorage(size());

    if (storage.isDense()) {
      double[] values = ((IntDoubleVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = ((IntDoubleVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        double value = entry.getDoubleValue();
        if (value >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntDoubleVectorStorage) storage).getIndices();
      double[] values = ((IntDoubleVectorStorage) storage).getValues();

      int size = ((IntDoubleVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntDoubleVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}
