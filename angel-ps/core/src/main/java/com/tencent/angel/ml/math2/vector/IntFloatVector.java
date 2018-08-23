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
import it.unimi.dsi.fastutil.floats.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.ArrayUtils;

import java.lang.Math;

public class IntFloatVector extends FloatVector implements IntKeyVector, SimpleVector {
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

  public IntFloatVector(int matrixId, int rowId, int clock, int dim,
    IntFloatVectorStorage storage) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.storage = storage;
  }

  public IntFloatVector(int dim, IntFloatVectorStorage storage) {
    this(0, 0, 0, dim, storage);
  }

  public float get(int idx) {
    return ((IntFloatVectorStorage) storage).get(idx);
  }

  public float[] get(int[] idxs) {
    return ((IntFloatVectorStorage) storage).get(idxs);
  }

  public void set(int idx, float value) {
    ((IntFloatVectorStorage) storage).set(idx, value);
  }

  public float max() {
    IntFloatVectorStorage idstorage = (IntFloatVectorStorage) storage;
    if (idstorage.size() == 0)
      return 0;
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
    IntFloatVectorStorage idstorage = (IntFloatVectorStorage) storage;
    if (idstorage.size() == 0)
      return 0;
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

  public int argmax() {
    IntFloatVectorStorage idstorage = (IntFloatVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    float maxval = Float.MIN_VALUE;
    int maxidx = -1;
    if (idstorage.isDense()) {
      float[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] > maxval) {
          maxval = val[idx];
          maxidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        float val = entry.getFloatValue();
        if (val > maxval) {
          maxval = val;
          maxidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      float[] val = idstorage.getValues();
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
    IntFloatVectorStorage idstorage = (IntFloatVectorStorage) storage;
    if (idstorage.size() == 0)
      return -1;
    float minval = Float.MAX_VALUE;
    int minidx = -1;
    if (idstorage.isDense()) {
      float[] val = idstorage.getValues();
      int length = val.length;
      for (int idx = 0; idx < length; idx++) {
        if (val[idx] < minval) {
          minval = val[idx];
          minidx = idx;
        }
      }
    } else if (idstorage.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = idstorage.entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        float val = entry.getFloatValue();
        if (val < minval) {
          minval = val;
          minidx = idx;
        }
      }
    } else {
      int[] indices = idstorage.getIndices();
      float[] val = idstorage.getValues();
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
    IntFloatVectorStorage dstorage = (IntFloatVectorStorage) storage;
    if (dstorage.size() == 0)
      return 0;
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
    IntFloatVectorStorage dstorage = (IntFloatVectorStorage) storage;
    if (dstorage.size() == 0)
      return 0;
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

  public int size() {
    return ((IntFloatVectorStorage) storage).size();
  }

  public int numZeros() {
    IntFloatVectorStorage dstorage = (IntFloatVectorStorage) storage;
    if (dstorage.size() == 0)
      return (int) dim;
    int numZero = 0;
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
    return (int) getDim() - numZero;
  }

  public IntFloatVector clone() {
    return new IntFloatVector(matrixId, rowId, clock, dim,
      ((IntFloatVectorStorage) storage).clone());
  }

  @Override public IntFloatVector copy() {
    return new IntFloatVector(matrixId, rowId, clock, dim,
      ((IntFloatVectorStorage) storage).copy());
  }

  @Override public IntFloatVectorStorage getStorage() {
    return (IntFloatVectorStorage) storage;
  }

  @Override public boolean hasKey(int idx) {
    return getStorage().hasKey(idx);
  }

  @Override public Vector filter(double threshold) {
    IntFloatSparseVectorStorage newStorage = new IntFloatSparseVectorStorage(size());

    if (storage.isDense()) {
      float[] values = ((IntFloatVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (Math.abs(value) >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntFloatVectorStorage) storage).getIndices();
      float[] values = ((IntFloatVectorStorage) storage).getValues();

      int size = ((IntFloatVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntFloatVector(matrixId, rowId, clock, getDim(), newStorage);
  }

  @Override public Vector ifilter(double threshold) {

    if (storage.isDense()) {
      float[] values = ((IntFloatVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (Math.abs(values[i]) <= threshold) {
          values[i] = 0;
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (Math.abs(value) <= threshold) {
          iter.remove();
        }
      }
    } else {
      int[] indices = ((IntFloatVectorStorage) storage).getIndices();
      float[] values = ((IntFloatVectorStorage) storage).getValues();

      int size = ((IntFloatVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (Math.abs(values[k]) <= threshold) {
          values = ArrayUtils.remove(values, k);
          indices = ArrayUtils.remove(indices, k);
        }
      }
    }

    return new IntFloatVector(matrixId, rowId, clock, getDim(), (IntFloatVectorStorage) storage);
  }

  @Override public Vector filterUp(double threshold) {
    IntFloatSparseVectorStorage newStorage = new IntFloatSparseVectorStorage(size());

    if (storage.isDense()) {
      float[] values = ((IntFloatVectorStorage) storage).getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] >= threshold) {
          newStorage.set(i, values[i]);
        }
      }
    } else if (storage.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVectorStorage) storage).entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        float value = entry.getFloatValue();
        if (value >= threshold) {
          newStorage.set(entry.getIntKey(), value);
        }
      }
    } else {
      int[] indices = ((IntFloatVectorStorage) storage).getIndices();
      float[] values = ((IntFloatVectorStorage) storage).getValues();

      int size = ((IntFloatVectorStorage) storage).size();
      for (int k = 0; k < size; k++) {
        if (values[k] >= threshold) {
          newStorage.set(indices[k], values[k]);
        }
      }
    }

    return new IntFloatVector(matrixId, rowId, clock, getDim(), newStorage);
  }
}
