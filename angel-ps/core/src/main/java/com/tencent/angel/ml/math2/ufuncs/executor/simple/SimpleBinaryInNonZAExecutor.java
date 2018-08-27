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
import com.tencent.angel.ml.math2.utils.Constant;

public class SimpleBinaryInNonZAExecutor {
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

  public static Vector apply(IntDoubleVector v1, IntDoubleVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getDoubleValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        double[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        double[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntDoubleVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        ObjectIterator<Int2DoubleMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2DoubleMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getDoubleValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        } else { // multi-rehash
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          ObjectIterator<Int2DoubleMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2DoubleMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getDoubleValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        double[] values = new double[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        double[] resValues = new double[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntFloatVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getFloatValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        float[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        float[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntDoubleVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        ObjectIterator<Int2FloatMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2FloatMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getFloatValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        } else { // multi-rehash
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          ObjectIterator<Int2FloatMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2FloatMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getFloatValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        double[] values = new double[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        double[] resValues = new double[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntLongVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getLongValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntDoubleVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        ObjectIterator<Int2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2LongMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getLongValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else { // multi-rehash
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          ObjectIterator<Int2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2LongMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getLongValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        double[] values = new double[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        double[] resValues = new double[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntDoubleVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2IntMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else { // multi-rehash
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2IntMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        double[] values = new double[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        double[] resValues = new double[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntFloatVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getFloatValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        float[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        float[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntFloatVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        ObjectIterator<Int2FloatMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2FloatMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getFloatValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        } else { // multi-rehash
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          ObjectIterator<Int2FloatMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2FloatMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getFloatValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        float[] values = new float[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntFloatVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        float[] resValues = new float[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntLongVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getLongValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntFloatVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        ObjectIterator<Int2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2LongMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getLongValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else { // multi-rehash
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          ObjectIterator<Int2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2LongMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getLongValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        float[] values = new float[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntFloatVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        float[] resValues = new float[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntFloatVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2IntMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else { // multi-rehash
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2IntMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        float[] values = new float[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntFloatVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        float[] resValues = new float[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntLongVector v1, IntLongVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getLongValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getLongValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntLongVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        ObjectIterator<Int2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2LongMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getLongValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else { // multi-rehash
          IntLongVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2LongMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getLongValue());
          }

          ObjectIterator<Int2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2LongMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntLongVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2LongMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getLongValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getLongValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        long[] values = new long[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntLongVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        long[] resValues = new long[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntLongVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getLongValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntLongVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2IntMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else { // multi-rehash
          IntLongVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2LongMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getLongValue());
          }

          ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2IntMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntLongVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2LongMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getLongValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        long[] values = new long[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntLongVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        long[] resValues = new long[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntIntVector v1, IntIntVector v2, Binary op) {
    if (v1.isDense() && v2.isDense()) {
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      int[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resValues[idx] = op.apply(entry.getIntValue(), v2Values[idx]);
          }
        } else {
          for (int i = 0; i < resValues.length; i++) {
            if (v1.getStorage().hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(0, v2Values[i]);
          }

          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
          }
        } else {
          IntIntVectorStorage v1Storage = v1.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v1Storage.hasKey(i)) {
              resValues[i] = op.apply(v1.get(i), v2Values[i]);
            } else {
              resValues[i] = op.apply(0, v2Values[i]);
            }
          }
        }

        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2IntMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getIntValue();
        }

        ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Int2IntMap.Entry entry = iter2.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else { // multi-rehash
          IntIntVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2IntMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getIntValue());
          }

          ObjectIterator<Int2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
          while (iter2.hasNext()) {
            Int2IntMap.Entry entry = iter2.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2IntMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getIntValue();
        }

        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], v2Values[i]);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() < 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
          IntIntVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2IntMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getIntValue());
          }

          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
          v1.setStorage(newStorage);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(resValues[idx], entry.getIntValue());
        }

        v1.setStorage(newStorage);
      } else {
        int[] v1Indices = v1.getStorage().getIndices();
        int[] idxiter = v2.getStorage().indexIterator().toIntArray();

        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        int[] values = new int[(int) (v1.size() + v2.size())];

        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      IntIntVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      int[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = v1Values[v1Pointor];
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }
      } else {
        int[] resIndices = new int[(int) (v1.size() + v2.size())];
        int[] resValues = new int[(int) (v1.size() + v2.size())];
        int globalPointor = 0;

        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
            globalPointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resIndices[globalPointor] = v1Indices[v1Pointor];
            resValues[globalPointor] = v1Values[v1Pointor];
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }
        newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);
      }

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongDoubleVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
        }
      } else { // multi-rehash
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        ObjectIterator<Long2DoubleMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2DoubleMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      double[] values = new double[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongDoubleSortedVectorStorage newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      double[] resValues = new double[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongFloatVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
      } else { // multi-rehash
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        ObjectIterator<Long2FloatMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2FloatMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      double[] values = new double[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongDoubleSortedVectorStorage newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      double[] resValues = new double[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongLongVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else { // multi-rehash
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        ObjectIterator<Long2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2LongMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      double[] values = new double[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongDoubleSortedVectorStorage newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      double[] resValues = new double[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else { // multi-rehash
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        ObjectIterator<Long2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2IntMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      double[] values = new double[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongDoubleSortedVectorStorage newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongDoubleVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      double[] resValues = new double[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongFloatVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
      } else { // multi-rehash
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        ObjectIterator<Long2FloatMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2FloatMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      float[] values = new float[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongFloatSortedVectorStorage newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongFloatVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      float[] resValues = new float[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongLongVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else { // multi-rehash
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        ObjectIterator<Long2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2LongMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      float[] values = new float[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongFloatSortedVectorStorage newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongFloatVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      float[] resValues = new float[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else { // multi-rehash
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        ObjectIterator<Long2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2IntMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      float[] values = new float[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongFloatSortedVectorStorage newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongFloatVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      float[] resValues = new float[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongLongVector v1, LongLongVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else { // multi-rehash
        LongLongVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2LongMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getLongValue());
        }

        ObjectIterator<Long2LongMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2LongMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongLongVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2LongMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getLongValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      long[] values = new long[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongLongSortedVectorStorage newStorage =
        new LongLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongLongVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      long[] resValues = new long[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongLongSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongLongVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else { // multi-rehash
        LongLongVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2LongMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getLongValue());
        }

        ObjectIterator<Long2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2IntMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongLongVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2LongMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getLongValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      long[] values = new long[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongLongSortedVectorStorage newStorage =
        new LongLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongLongVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      long[] resValues = new long[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongLongSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(LongIntVector v1, LongIntVector v2, Binary op) {
    if (v1.isSparse() && v2.isSparse()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        // no rehashor one onle rehash is required, nothing to optimization
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          v1.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else { // multi-rehash
        LongIntVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2IntMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getIntValue());
        }

        ObjectIterator<Long2IntMap.Entry> iter2 = v2.getStorage().entryIterator();
        while (iter2.hasNext()) {
          Long2IntMap.Entry entry = iter2.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      if (v1.size() + v2.size() < 1.5 * capacity) {
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else {
        LongIntVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2IntMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getIntValue());
        }

        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
        v1.setStorage(newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      long[] v1Indices = v1.getStorage().getIndices();
      long[] idxiter = v2.getStorage().indexIterator().toLongArray();

      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      int[] values = new int[(int) (v1.size() + v2.size())];

      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongIntSortedVectorStorage newStorage =
        new LongIntSortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);
      v1.setStorage(newStorage);
    } else if (v1.isSorted() && v2.isSorted()) {
      LongIntVectorStorage newStorage;
      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      long[] v1Indices = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      long[] resIndices = new long[(int) (v1.size() + v2.size())];
      int[] resValues = new int[(int) (v1.size() + v2.size())];
      int globalPointor = 0;

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
          v1Pointor++;
          v2Pointor++;
          globalPointor++;
        } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
          resIndices[globalPointor] = v1Indices[v1Pointor];
          resValues[globalPointor] = v1Values[v1Pointor];
          v1Pointor++;
          globalPointor++;
        } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
          resIndices[globalPointor] = v2Indices[v2Pointor];
          resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
          v2Pointor++;
          globalPointor++;
        }
      }
      newStorage =
        new LongIntSortedVectorStorage(v1.getDim(), globalPointor, resIndices, resValues);

      v1.setStorage(newStorage);
    } else {
      throw new AngelException("The operation is not support!");
    }

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          resValues[entry.getIntKey()] = entry.getDoubleValue();
        }

        for (int idx : v2Indices) {
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
          IntDoubleVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2DoubleMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getDoubleValue());
          }

          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
          v1.setStorage(newStorage);
        }
      }
    } else { // sorted
      int[] v1Indices = v1.getStorage().getIndices();
      int[] v2Indices = v2.getIndices();
      int size1 = v1.size();
      int size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        double[] v1Values = v1.getStorage().getValues();
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        for (int i = 0; i < size1; i++) {
          resValues[v1Indices[i]] = v1Values[i];
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        for (int i = 0; i < indices.length; i++)
          indices[i] = 0;
        double[] values = new double[indices.length];
        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntDoubleSortedVectorStorage newStorage =
          new IntDoubleSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    }
    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          resValues[entry.getIntKey()] = entry.getFloatValue();
        }

        for (int idx : v2Indices) {
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
          IntFloatVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2FloatMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getFloatValue());
          }

          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
          v1.setStorage(newStorage);
        }
      }
    } else { // sorted
      int[] v1Indices = v1.getStorage().getIndices();
      int[] v2Indices = v2.getIndices();
      int size1 = v1.size();
      int size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        float[] v1Values = v1.getStorage().getValues();
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        for (int i = 0; i < size1; i++) {
          resValues[v1Indices[i]] = v1Values[i];
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        for (int i = 0; i < indices.length; i++)
          indices[i] = 0;
        float[] values = new float[indices.length];
        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntFloatSortedVectorStorage newStorage =
          new IntFloatSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    }
    return v1;
  }

  public static Vector apply(IntLongVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          resValues[entry.getIntKey()] = entry.getLongValue();
        }

        for (int idx : v2Indices) {
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
          IntLongVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2LongMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getLongValue());
          }

          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
          v1.setStorage(newStorage);
        }
      }
    } else { // sorted
      int[] v1Indices = v1.getStorage().getIndices();
      int[] v2Indices = v2.getIndices();
      int size1 = v1.size();
      int size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        long[] v1Values = v1.getStorage().getValues();
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        for (int i = 0; i < size1; i++) {
          resValues[v1Indices[i]] = v1Values[i];
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        for (int i = 0; i < indices.length; i++)
          indices[i] = 0;
        long[] values = new long[indices.length];
        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntLongSortedVectorStorage newStorage =
          new IntLongSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    }
    return v1;
  }

  public static Vector apply(IntIntVector v1, IntDummyVector v2, Binary op) {
    if (v1.isDense()) {
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();
      if (!op.isKeepStorage() && ((v1.size() + v2.size()) * Constant.intersectionCoeff
        > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          resValues[entry.getIntKey()] = entry.getIntValue();
        }

        for (int idx : v2Indices) {
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            v1.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
          IntIntVectorStorage newStorage =
            v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

          ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
          while (iter1.hasNext()) {
            Int2IntMap.Entry entry = iter1.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, entry.getIntValue());
          }

          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
          v1.setStorage(newStorage);
        }
      }
    } else { // sorted
      int[] v1Indices = v1.getStorage().getIndices();
      int[] v2Indices = v2.getIndices();
      int size1 = v1.size();
      int size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
        > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        int[] v1Values = v1.getStorage().getValues();
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        for (int i = 0; i < size1; i++) {
          resValues[v1Indices[i]] = v1Values[i];
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(resValues[idx], 1);
        }

        v1.setStorage(newStorage);
      } else {
        int[] indices = new int[(int) (v1.size() + v2.size())];
        System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
        System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
        IntAVLTreeSet avl = new IntAVLTreeSet(indices);

        IntBidirectionalIterator iter = avl.iterator();
        for (int i = 0; i < indices.length; i++)
          indices[i] = 0;
        int[] values = new int[indices.length];
        int i = 0;
        while (iter.hasNext()) {
          int idx = iter.nextInt();
          indices[i] = idx;
          values[i] = op.apply(v1.get(idx), v2.get(idx));
          i++;
        }

        while (i < indices.length) {
          indices[i] = 0;
          i++;
        }

        IntIntSortedVectorStorage newStorage =
          new IntIntSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
        v1.setStorage(newStorage);
      }
    }
    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      long size = v2.size();
      if (v1.size() + v2.size() < 1.5 * capacity) {
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), 1));
        }
      } else {
        LongDoubleVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2DoubleMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getDoubleValue());
        }

        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }
        v1.setStorage(newStorage);
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();
      long size1 = v1.size();
      long size2 = v2.size();
      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      for (int i = 0; i < indices.length; i++)
        indices[i] = 0;
      double[] values = new double[indices.length];
      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongDoubleSortedVectorStorage newStorage =
        new LongDoubleSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
      v1.setStorage(newStorage);
    }
    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      long size = v2.size();
      if (v1.size() + v2.size() < 1.5 * capacity) {
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), 1));
        }
      } else {
        LongFloatVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2FloatMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getFloatValue());
        }

        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }
        v1.setStorage(newStorage);
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();
      long size1 = v1.size();
      long size2 = v2.size();
      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      for (int i = 0; i < indices.length; i++)
        indices[i] = 0;
      float[] values = new float[indices.length];
      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongFloatSortedVectorStorage newStorage =
        new LongFloatSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
      v1.setStorage(newStorage);
    }
    return v1;
  }

  public static Vector apply(LongLongVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      long size = v2.size();
      if (v1.size() + v2.size() < 1.5 * capacity) {
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), 1));
        }
      } else {
        LongLongVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2LongMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getLongValue());
        }

        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }
        v1.setStorage(newStorage);
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();
      long size1 = v1.size();
      long size2 = v2.size();
      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      for (int i = 0; i < indices.length; i++)
        indices[i] = 0;
      long[] values = new long[indices.length];
      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongLongSortedVectorStorage newStorage =
        new LongLongSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
      v1.setStorage(newStorage);
    }
    return v1;
  }

  public static Vector apply(LongIntVector v1, LongDummyVector v2, Binary op) {
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();
      // to avoid multi-rehash
      int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
      long size = v2.size();
      if (v1.size() + v2.size() < 1.5 * capacity) {
        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          v1.set(idx, op.apply(v1.get(idx), 1));
        }
      } else {
        LongIntVectorStorage newStorage =
          v1.getStorage().emptySparse((int) (v1.size() + v2.size()));

        ObjectIterator<Long2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Long2IntMap.Entry entry = iter1.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, entry.getIntValue());
        }

        for (int i = 0; i < size; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }
        v1.setStorage(newStorage);
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();
      long size1 = v1.size();
      long size2 = v2.size();
      long[] indices = new long[(int) (v1.size() + v2.size())];
      System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
      System.arraycopy(v2Indices, 0, indices, (int) v1.size(), (int) v2.size());
      LongAVLTreeSet avl = new LongAVLTreeSet(indices);

      LongBidirectionalIterator iter = avl.iterator();
      for (int i = 0; i < indices.length; i++)
        indices[i] = 0;
      int[] values = new int[indices.length];
      int i = 0;
      while (iter.hasNext()) {
        long idx = iter.nextLong();
        indices[i] = idx;
        values[i] = op.apply(v1.get(idx), v2.get(idx));
        i++;
      }

      while (i < indices.length) {
        indices[i] = 0;
        i++;
      }

      LongIntSortedVectorStorage newStorage =
        new LongIntSortedVectorStorage(v1.getDim(), avl.size(), indices, values);
      v1.setStorage(newStorage);
    }
    return v1;
  }

}