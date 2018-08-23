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


package com.tencent.angel.ml.math2.ufuncs.executor.simple;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.math2.utils.Constant;

public class SimpleBinaryOutZAExecutor {
  public static Vector apply(Vector v1, Vector v2, Binary op) {
    if (v1 instanceof IntDoubleVector && v2 instanceof IntDoubleVector) {
      return apply((IntDoubleVector) v1, (IntDoubleVector) v2, op);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntFloatVector) {
      return apply((IntDoubleVector) v1, (IntFloatVector) v2, op);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntLongVector) {
      return apply((IntDoubleVector) v1, (IntLongVector) v2, op);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntIntVector) {
      return apply((IntDoubleVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntDummyVector) {
      return apply((IntDoubleVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntFloatVector) {
      return apply((IntFloatVector) v1, (IntFloatVector) v2, op);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntLongVector) {
      return apply((IntFloatVector) v1, (IntLongVector) v2, op);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntIntVector) {
      return apply((IntFloatVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntDummyVector) {
      return apply((IntFloatVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof IntLongVector && v2 instanceof IntLongVector) {
      return apply((IntLongVector) v1, (IntLongVector) v2, op);
    } else if (v1 instanceof IntLongVector && v2 instanceof IntIntVector) {
      return apply((IntLongVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof IntLongVector && v2 instanceof IntDummyVector) {
      return apply((IntLongVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof IntIntVector && v2 instanceof IntIntVector) {
      return apply((IntIntVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof IntIntVector && v2 instanceof IntDummyVector) {
      return apply((IntIntVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongDoubleVector) {
      return apply((LongDoubleVector) v1, (LongDoubleVector) v2, op);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongFloatVector) {
      return apply((LongDoubleVector) v1, (LongFloatVector) v2, op);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongLongVector) {
      return apply((LongDoubleVector) v1, (LongLongVector) v2, op);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongIntVector) {
      return apply((LongDoubleVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongDummyVector) {
      return apply((LongDoubleVector) v1, (LongDummyVector) v2, op);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongFloatVector) {
      return apply((LongFloatVector) v1, (LongFloatVector) v2, op);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongLongVector) {
      return apply((LongFloatVector) v1, (LongLongVector) v2, op);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongIntVector) {
      return apply((LongFloatVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongDummyVector) {
      return apply((LongFloatVector) v1, (LongDummyVector) v2, op);
    } else if (v1 instanceof LongLongVector && v2 instanceof LongLongVector) {
      return apply((LongLongVector) v1, (LongLongVector) v2, op);
    } else if (v1 instanceof LongLongVector && v2 instanceof LongIntVector) {
      return apply((LongLongVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof LongLongVector && v2 instanceof LongDummyVector) {
      return apply((LongLongVector) v1, (LongDummyVector) v2, op);
    } else if (v1 instanceof LongIntVector && v2 instanceof LongIntVector) {
      return apply((LongIntVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof LongIntVector && v2 instanceof LongDummyVector) {
      return apply((LongIntVector) v1, (LongDummyVector) v2, op);
    } else {
      throw new AngelException("Vector type is not support!");
    }
  }

  private static Vector apply(IntDoubleVector v1, IntDoubleVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getDoubleValue());
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getDoubleValue()));
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        double[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        double[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        double[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        double[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        IntDoubleVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getFloatValue()));
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        IntFloatVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getLongValue()));
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        IntLongVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        IntIntVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntDoubleVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getFloatValue()));
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();

        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];
        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntFloatVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        IntFloatVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntFloatVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getLongValue()));
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();

        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];
        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntFloatVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        IntLongVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntFloatVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();

        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];
        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntFloatVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        IntIntVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntFloatVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        long[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getLongValue()));
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        long[] resValues = newStorage.getValues();

        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
        }
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getLongValue(), v2Values[idx]);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        long[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];
        IntLongVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntLongVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        long[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        IntLongVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntLongVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        long[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        long[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        long[] resValues = newStorage.getValues();

        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
        }
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getLongValue(), v2Values[idx]);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        long[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];
        IntLongVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntLongVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        long[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        IntIntVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntLongVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        long[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(IntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      int[] resValues = res.getStorage().getValues();
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if (op.isKeepStorage() || v2.size() > Constant.sparseDenseStorageThreshold * v2.getDim()) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] newValues = newStorage.getValues();
        int[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        int[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if (op.isKeepStorage() || v2.size() > Constant.sortedDenseStorageThreshold * v2.getDim()) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] newValues = newStorage.getValues();
        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptySorted((int) (v2.size()));
        int[] resIndices = newStorage.getIndices();
        int[] resValues = newStorage.getValues();

        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          resIndices[i] = idx;
          resValues[i] = op.apply(v1Values[idx], v2Values[i]);
        }

        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        ObjectIterator<Int2IntMap.Entry> iter = res.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          entry.setValue(op.apply(entry.getIntValue(), v2Values[idx]));
        }
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] newValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getIntValue(), v2Values[idx]);
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()) {
        res = v1.copy();
        int[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        IntIntVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2IntMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Int2IntMap.Entry> iter = res.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        int[] resValues = new int[v2Values.length];
        IntIntVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        IntIntVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        int[] resValues = res.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        IntIntVectorStorage storage = v2.getStorage();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 > size2) {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int[] resIndices = ArrayCopy.copy(vIndices);
        int[] resValues = new int[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        IntIntVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      } else {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        int[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        LongDoubleVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        LongFloatVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        LongLongVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];
        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        double[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        LongIntVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        double[] resValues = new double[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongDoubleVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];
        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongFloatVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        LongFloatVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongFloatVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];
        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongFloatVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        LongLongVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongFloatVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];
        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongFloatVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        float[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        LongIntVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        float[] resValues = new float[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongFloatVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2LongMap.Entry> iter = res.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2LongMap.Entry> iter = res.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];
        LongLongVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongLongVectorStorage newStorage =
          new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        long[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        LongLongVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongLongVectorStorage newStorage =
          new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        long[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2LongMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2LongMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];
        LongLongVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongLongVectorStorage newStorage =
          new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        long[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        LongIntVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        long[] resValues = new long[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongLongVectorStorage newStorage =
          new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        long[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  private static Vector apply(LongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() > v2.size()) {
        LongIntVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2IntMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage() || v1.size() < v2.size() * Constant.sparseSortedThreshold) {
        res = v1.copy();
        ObjectIterator<Long2IntMap.Entry> iter = res.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
          } else {
            iter.remove();
          }
        }
      } else {
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        int[] resValues = new int[v2Values.length];
        LongIntVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = vIndices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(storage.get(idx), v2Values[i]);
          }
        }

        LongIntVectorStorage newStorage =
          new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.size()) {
        res = v1.copy();
        int[] resValues = res.getStorage().getValues();

        long[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        LongIntVectorStorage storage = v2.getStorage();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          long idx = v1Indices[i];
          if (storage.hasKey(idx)) {
            resValues[i] = op.apply(v1Values[i], storage.get(idx));
          } else {
            resValues[i] = 0;
          }
        }
      } else {
        LongIntVectorStorage newStorage = v1.getStorage().emptySparse((int) v2.size());
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 > size2) {
        long[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long[] resIndices = ArrayCopy.copy(vIndices);
        int[] resValues = new int[v2Values.length];

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (vIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
        LongIntVectorStorage newStorage =
          new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        int[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }



  public static Vector apply(IntDoubleVector v1, IntDummyVector v2, Binary op) {
    IntDoubleVector res;

    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        int i = 0;
        for (int idx : vIndices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(IntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVector res;

    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];

        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        int i = 0;
        for (int idx : vIndices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(IntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVector res;

    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];

        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        int i = 0;
        for (int idx : vIndices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        long[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(IntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVector res;

    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];

        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        int i = 0;
        for (int idx : vIndices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Int2IntMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        int[] resIndices = res.getStorage().getIndices();
        int[] resValues = res.getStorage().getValues();
        int[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVector res;

    if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        LongDoubleSortedVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        double[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];
        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        LongDoubleSortedVectorStorage newStorage =
          new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVector res;

    if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        LongFloatSortedVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        float[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];
        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        LongFloatSortedVectorStorage newStorage =
          new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVector res;

    if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        LongLongSortedVectorStorage newStorage =
          new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2LongMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        long[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];
        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        LongLongSortedVectorStorage newStorage =
          new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVector res;

    if (v1.isSparse()) {
      if (v1.size() > v2.size() && !op.isKeepStorage()) {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = resIndices[i];
          if (v1.getStorage().hasKey(idx)) {
            resValues[i] = op.apply(v1.get(idx), 1);
          }
        }

        LongIntSortedVectorStorage newStorage =
          new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        return new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      } else {
        res = v1.copy();
        ObjectIterator<Long2IntMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            entry.setValue(op.apply(v1.get(idx), 1));
          } else {
            iter.remove();
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 < size2) {
        res = v1.copy();
        long[] resIndices = res.getStorage().getIndices();
        int[] resValues = res.getStorage().getValues();
        long[] vIndices = v2.getIndices();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (resIndices[v1Pointor] == vIndices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(resValues[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < vIndices[v2Pointor]) {
            resValues[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        long[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];
        long[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == resIndices[v2Pointor]) {
            resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v2Pointor] < v1Indices[v1Pointor]) {
            v2Pointor++;
          } else { // resIndices[v2Pointor] > v1Indices[v1Pointor]
            v1Pointor++;
          }
        }

        LongIntSortedVectorStorage newStorage =
          new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }


}
