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

public class SimpleBinaryInZAExecutor {
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
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntDoubleVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getDoubleValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntDoubleVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      double[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      IntDoubleVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntDoubleVector v1, IntFloatVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntFloatVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntFloatVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      float[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      IntFloatVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntDoubleVector v1, IntLongVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntLongVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntLongVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      long[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      IntLongVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntDoubleVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      int[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      IntIntVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntFloatVector v1, IntFloatVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntFloatVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getFloatValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntFloatVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      float[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      IntFloatVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntFloatVector v1, IntLongVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntLongVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntLongVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      long[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      IntLongVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntFloatVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      int[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      IntIntVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntLongVector v1, IntLongVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntLongVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getLongValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntLongVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
      long[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      IntLongVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntLongVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
      int[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      IntIntVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(IntIntVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      int[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] newValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newValues[idx] = op.apply(v1Values[idx], entry.getIntValue());
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isDense() && v2.isSorted()) {
      int[] v1Values = v1.getStorage().getValues();
      if (v2.size() < Constant.sparseDenseStorageThreshold * v2.getDim()
        || v1.getDim() < Constant.denseStorageThreshold) {
        // slower but memory efficient, for small vector only
        IntIntVectorStorage v2storage = v2.getStorage();
        for (int i = 0; i < v1Values.length; i++) {
          if (v2storage.hasKey(i)) {
            v1Values[i] = op.apply(v1Values[i], v2.get(i));
          } else {
            v1Values[i] = 0;
          }
        }
      } else { // faster but not memory efficient
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] newValues = newStorage.getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          newValues[idx] = op.apply(v1Values[idx], v2Values[i]);
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
      int[] v2Values = v2.getStorage().getValues();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        entry.setValue(op.apply(entry.getIntValue(), v2Values[idx]));
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        v1Values[i] = op.apply(v1Values[i], v2Values[v1Indices[i]]);
      }
    } else if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      int[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      IntIntVectorStorage storage = v2.getStorage();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        int idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongDoubleVector v1, LongDoubleVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      LongDoubleVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongDoubleVector v1, LongFloatVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      LongFloatVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongDoubleVector v1, LongLongVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      LongLongVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongDoubleVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      LongIntVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongFloatVector v1, LongFloatVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      LongFloatVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongFloatVector v1, LongLongVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      LongLongVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongFloatVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      LongIntVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongLongVector v1, LongLongVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      LongLongVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongLongVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      LongIntVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  private static Vector apply(LongIntVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && !v2.isDense()) {
      ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
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
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      LongIntVectorStorage storage = v2.getStorage();
      long size = v1.size();
      for (int i = 0; i < size; i++) {
        long idx = v1Indices[i];
        if (storage.hasKey(idx)) {
          v1Values[i] = op.apply(v1Values[i], v2.get(idx));
        } else {
          v1Values[i] = 0;
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }



  public static Vector apply(IntDoubleVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        for (int idx : v2Indices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        v1.setStorage(newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];

        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        int i = 0;
        for (int idx : v2Indices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
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
        v1.setStorage(newStorage);
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
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
      if (v1.size() < v2.size()) {
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        double[] resValues = new double[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (resIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
      }
    }

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        float[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        for (int idx : v2Indices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        v1.setStorage(newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];

        float[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        int i = 0;
        for (int idx : v2Indices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
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
        v1.setStorage(newStorage);
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
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
      if (v1.size() < v2.size()) {
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        float[] resValues = new float[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (resIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
      }
    }

    return v1;
  }

  public static Vector apply(IntLongVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        long[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        for (int idx : v2Indices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        v1.setStorage(newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];

        long[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        int i = 0;
        for (int idx : v2Indices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
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
        v1.setStorage(newStorage);
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
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
      if (v1.size() < v2.size()) {
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        long[] resValues = new long[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (resIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
      }
    }

    return v1;
  }

  public static Vector apply(IntIntVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      if (op.isKeepStorage()) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        int[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        for (int idx : v2Indices) {
          resValues[idx] = op.apply(v1Values[idx], 1);
        }
        v1.setStorage(newStorage);
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];

        int[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();
        int i = 0;
        for (int idx : v2Indices) {
          resValues[i] = op.apply(v1Values[idx], 1);
          i++;
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
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
        v1.setStorage(newStorage);
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
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
      if (v1.size() < v2.size()) {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getIndices();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            v1Values[v1Pointor] = 0;
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = ArrayCopy.copy(v2.getIndices());
        int[] resValues = new int[resIndices.length];
        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
          if (resIndices[v1Pointor] == v1Indices[v2Pointor]) {
            resValues[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
            v1Pointor++;
            v2Pointor++;
          } else if (resIndices[v1Pointor] < v1Indices[v2Pointor]) {
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            v2Pointor++;
          }
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) v2.size(), resIndices, resValues);
        v1.setStorage(newStorage);
      }
    }

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        if (v2.hasKey(idx)) {
          entry.setValue(op.apply(v1.get(idx), 1));
        } else {
          iter.remove();
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getIndices();

      while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    }

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        if (v2.hasKey(idx)) {
          entry.setValue(op.apply(v1.get(idx), 1));
        } else {
          iter.remove();
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getIndices();

      while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    }

    return v1;
  }

  public static Vector apply(LongLongVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        if (v2.hasKey(idx)) {
          entry.setValue(op.apply(v1.get(idx), 1));
        } else {
          iter.remove();
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getIndices();

      while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    }

    return v1;
  }

  public static Vector apply(LongIntVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        if (v2.hasKey(idx)) {
          entry.setValue(op.apply(v1.get(idx), 1));
        } else {
          iter.remove();
        }
      }
    } else { // sorted
      int v1Pointor = 0;
      int v2Pointor = 0;
      long[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getIndices();

      while (v1Pointor < v1.size() && v2Pointor < v2.size()) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = op.apply(v1Values[v1Pointor], 1);
          v1Pointor++;
          v2Pointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          v1Values[v1Pointor] = 0;
          v1Pointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          v2Pointor++;
        }
      }
    }

    return v1;
  }


}