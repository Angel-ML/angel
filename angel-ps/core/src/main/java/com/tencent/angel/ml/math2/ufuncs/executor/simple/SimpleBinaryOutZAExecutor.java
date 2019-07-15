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
import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.math2.utils.Constant;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

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
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] resValues = newStorage.getValues();
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getDoubleValue());
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getDoubleValue());
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getDoubleValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        double[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        double[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        double[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        double[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        double[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntDoubleVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntDoubleVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntDoubleVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntDoubleVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] resValues = newStorage.getValues();
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getFloatValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        double[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] resValues = newStorage.getValues();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getLongValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        double[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] resValues = newStorage.getValues();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        double[] newValues = newStorage.getValues();
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          double[] newValues = newStorage.getValues();
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          double[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
        }
      } else {
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        double[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        double[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntDoubleVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      float[] resValues = newStorage.getValues();
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
        }
      } else {
        if (op.isKeepStorage()) {
          float[] newValues = newStorage.getValues();
          float[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
          }
        } else {
          float[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getFloatValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          float[] newValues = newStorage.getValues();
          float[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          float[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getFloatValue(), v2Values[idx]));
        }
      } else {
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        float[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        float[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        float[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        float[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          IntFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          IntFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntFloatVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = new float[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      float[] resValues = newStorage.getValues();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
      } else {
        if (op.isKeepStorage()) {
          float[] newValues = newStorage.getValues();
          float[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
          }
        } else {
          float[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getLongValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          float[] newValues = newStorage.getValues();
          float[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          float[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getFloatValue(), v2Values[idx]));
        }
      } else {
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        float[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        float[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          IntFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          IntFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = new float[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      float[] resValues = newStorage.getValues();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
      } else {
        if (op.isKeepStorage()) {
          float[] newValues = newStorage.getValues();
          float[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
          }
        } else {
          float[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        float[] newValues = newStorage.getValues();
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          float[] newValues = newStorage.getValues();
          float[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          float[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getFloatValue(), v2Values[idx]));
        }
      } else {
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        float[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        float[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntFloatVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = new float[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVectorStorage newStorage = (IntLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      long[] resValues = newStorage.getValues();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
      } else {
        if (op.isKeepStorage()) {
          long[] newValues = newStorage.getValues();
          long[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
          }
        } else {
          long[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getLongValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          long[] newValues = newStorage.getValues();
          long[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          long[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getLongValue(), v2Values[idx]));
        }
      } else {
        long[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        long[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getLongValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        long[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        long[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        IntLongVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        IntLongVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          IntLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          IntLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntLongVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = new long[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVectorStorage newStorage = (IntLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      long[] resValues = newStorage.getValues();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
      } else {
        if (op.isKeepStorage()) {
          long[] newValues = newStorage.getValues();
          long[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
          }
        } else {
          long[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        long[] newValues = newStorage.getValues();
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          long[] newValues = newStorage.getValues();
          long[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          long[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getLongValue(), v2Values[idx]));
        }
      } else {
        long[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getLongValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        long[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        long[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntLongVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntLongVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = new long[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(IntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVectorStorage newStorage = (IntIntVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      int[] resValues = newStorage.getValues();
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int idx = 0; idx < length; idx++) {
        resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        int[] newValues = newStorage.getValues();
        int[] v1Values = v1.getStorage().getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
      } else {
        if (op.isKeepStorage()) {
          int[] newValues = newStorage.getValues();
          int[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
          }
        } else {
          int[] v1Values = v1.getStorage().getValues();
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1Values[idx], entry.getIntValue()));
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      if ((v2.isSparse() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
          (v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
        // dense preferred, KeepStorage is guaranteed
        int[] newValues = newStorage.getValues();
        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = vIndices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
      } else {
        if (op.isKeepStorage()) {
          int[] newValues = newStorage.getValues();
          int[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
          }
        } else {
          int[] v1Values = v1.getStorage().getValues();
          int[] vIndices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = vIndices[i];
            newStorage.set(idx, op.apply(v1Values[idx], v2Values[i]));
          }
        }
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sparseDenseStorageThreshold * v1.getDim()) {
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(entry.getIntValue(), v2Values[idx]));
        }
      } else {
        int[] newValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        int[] v2Values = v2.getStorage().getValues();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(entry.getIntValue(), v2Values[idx]);
        }
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage() || v1.size() <= Constant.sortedDenseStorageThreshold * v1.getDim()) {
        int[] resValues = newStorage.getValues();
        int[] resIndices = newStorage.getIndices();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          resIndices[i] = v1Indices[i];
          resValues[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
        }
        newStorage.setSize(resIndices.length);
      } else {
        int[] newValues = newStorage.getValues();
        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] v2Values = v2.getStorage().getValues();
        int size = v1.size();
        for (int k = 0; k < size; k++) {
          int idx = v1Indices[k];
          newValues[idx] = op.apply(v1Values[k], v2Values[idx]);
        }
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntIntVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        IntIntVectorStorage storage = v1.getStorage();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        IntIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntIntVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          IntIntVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        IntIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          int[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();

          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          IntIntVectorStorage storage = v2.getStorage();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          int[] resValues = new int[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v2Indices);
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongDoubleVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongDoubleVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongDoubleVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getDoubleValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongDoubleVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongDoubleVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongDoubleVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongDoubleVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongDoubleVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = new double[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongFloatVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          LongFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          LongFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getFloatValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongFloatVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = new float[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          LongFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          LongFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = new float[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongFloatVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongFloatVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongFloatVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = new float[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVectorStorage newStorage = (LongLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        LongLongVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        LongLongVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongLongVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          LongLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          LongLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getLongValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongLongVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = new long[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVectorStorage newStorage = (LongLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongLongVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongLongVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getLongValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongLongVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongLongVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = new long[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  private static Vector apply(LongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVectorStorage newStorage = (LongIntVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        }
      } else {// preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongIntVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        LongIntVectorStorage storage = v1.getStorage();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        LongIntVectorStorage v2storage = v2.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(entry.getIntValue(), v2.get(idx)));
          }
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v2.size
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongIntVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
          newStorage.setSize((int) v2.getSize());
        } else {//sparse preferred
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          LongIntVectorStorage v1storage = v1.getStorage();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            if (v1storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        LongIntVectorStorage v1storage = v1.getStorage();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v1storage.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1storage.get(idx), entry.getIntValue()));
          }
        }
      } else {//dense preferred
        if (op.isKeepStorage()) {// sorted preferred v1.size
          long[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();

          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              resIndices[i] = idx;
              resValues[i] = op.apply(v1Values[i], storage.get(idx));
            }
          }
          newStorage.setSize((int) v1.getSize());
        } else {//dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          LongIntVectorStorage storage = v2.getStorage();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            if (storage.hasKey(idx)) {
              newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));
            }
          }
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {//sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          int[] resValues = new int[v2Values.length];

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// sparse preferred
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {// sorted v2.size
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v2Indices);
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {// dense preferred
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > vIndices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }


  public static Vector apply(IntDoubleVector v1, IntDummyVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isDense()) {
      if (op.isKeepStorage() || v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) {
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
      } else {
        double[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          newStorage.set(idx, op.apply(v1Values[idx], 1));
        }
      }
    } else if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          double[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(IntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isDense()) {
      if (op.isKeepStorage() || v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) {
        float[] resValues = newStorage.getValues();

        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
      } else {
        float[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          newStorage.set(idx, op.apply(v1Values[idx], 1));
        }
      }
    } else if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          float[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(IntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVectorStorage newStorage = (IntLongVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isDense()) {
      if (op.isKeepStorage() || v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) {
        long[] resValues = newStorage.getValues();

        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
      } else {
        long[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          newStorage.set(idx, op.apply(v1Values[idx], 1));
        }
      }
    } else if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          long[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(IntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVectorStorage newStorage = (IntIntVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isDense()) {
      if (op.isKeepStorage() || v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) {
        int[] resValues = newStorage.getValues();

        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
      } else {
        int[] v1Values = v1.getStorage().getValues();
        int[] vIndices = v2.getIndices();
        for (int idx : vIndices) {
          newStorage.set(idx, op.apply(v1Values[idx], 1));
        }
      }
    } else if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        int[] v2Indices = v2.getIndices();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          int[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();
          int[] resIndices = ArrayCopy.copy(v2.getIndices());
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();
          int[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(LongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          double[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          double[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          double[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(LongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          float[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          float[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          float[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(LongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVectorStorage newStorage = (LongLongVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          long[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          long[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          long[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }

  public static Vector apply(LongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVectorStorage newStorage = (LongIntVectorStorage) StorageSwitch.apply(v1, v2, op);

    if (v1.isSparse()) {
      if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
        // sparse preferred, keep storage guaranteed
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // sparse preferred, keep storage guaranteed
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
        // preferred dense
        long[] v2Indices = v2.getIndices();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          if (v1.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      } else { // preferred dense
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          if (v2.hasKey(idx)) {
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      if (size1 >= size2 && size2 <= Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          int[] resValues = newStorage.getValues();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 <= size2 && size1 <= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else if (size1 > size2 && size2 > Constant.sortedDenseStorageThreshold * v2.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();
          long[] resIndices = ArrayCopy.copy(v2.getIndices());
          int[] resValues = newStorage.getValues();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v2Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v2Indices[v2Pointor] < v1Indices[v1Pointor]) {
              v2Pointor++;
            } else { // v2Indices[v2Pointor] > v1Indices[v1Pointor]
              v1Pointor++;
            }
          }

          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] resIndices = ArrayCopy.copy(v1Indices);
          int[] resValues = newStorage.getValues();
          long[] v2Indices = v2.getIndices();


          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) v1.size(), resIndices, resValues);
        } else {//dense or sparse
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long[] v2Indices = v2.getIndices();

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              v2Pointor++;
            }
          }
        }
      }
    }

    return new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
  }


}