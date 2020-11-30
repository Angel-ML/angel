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
import com.tencent.angel.ml.math2.storage.IntDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.utils.Constant;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntDummyVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongDummyVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

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
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = newStorage.getValues();
      double[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = newStorage.getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getDoubleValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        double[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();
        double[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getDoubleValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getDoubleValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = newStorage.getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = newStorage.getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getFloatValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        float[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();
        float[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = newStorage.getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = newStorage.getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getLongValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = newStorage.getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = newStorage.getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = newStorage.getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = newStorage.getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getFloatValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        float[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();
        float[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = newStorage.getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = newStorage.getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getLongValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = newStorage.getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = newStorage.getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getFloatValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVectorStorage newStorage = (IntLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = newStorage.getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = newStorage.getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getLongValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        long[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getLongValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        long[] resValues = newStorage.getValues();
        long[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVectorStorage newStorage = (IntLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = newStorage.getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = newStorage.getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getLongValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        long[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVectorStorage newStorage = (IntIntVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense() && v2.isDense()) {
      int[] v1Values = newStorage.getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < v1Values.length; idx++) {
        v1Values[idx] = op.apply(v1Values[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      int[] v1Values = newStorage.getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        v1Values[idx] = op.apply(v1Values[idx], entry.getIntValue());
      }
    } else if (v1.isDense() && v2.isSorted()) {
      int[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        int idx = v2Indices[i];
        v1Values[idx] = op.apply(v1Values[idx], v2Values[i]);
      }
    } else if (v1.isSparse() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] v2Values = v2.getStorage().getValues();

        if (v1.size() < Constant.denseLoopThreshold * v1.getDim()) {
          for (int i = 0; i < dim; i++) {
            newStorage.set(i, op.apply(0, v2Values[i]));
          }
          ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(entry.getIntValue(), v2Values[idx]));
          }
        } else {
          for (int i = 0; i < dim; i++) {
            if (v1.getStorage().hasKey(i)) {
              newStorage.set(i, op.apply(v1.get(i), v2Values[i]));
            } else {
              newStorage.set(i, op.apply(0, v2Values[i]));
            }
          }
        }
      } else {
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
      }
    } else if (v1.isSorted() && v2.isDense()) {
      if (op.isKeepStorage()) {
        int dim = v1.getDim();
        int[] resIndices = newStorage.getIndices();
        int[] resValues = newStorage.getValues();
        int[] v2Values = v2.getStorage().getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        for (int i = 0; i < dim; i++) {
          resIndices[i] = i;
          resValues[i] = op.apply(0, v2Values[i]);
        }

        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = op.apply(v1Values[i], v2Values[idx]);
        }
      } else {
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
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      int v1Size = v1.size();
      int v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] idxiter = v2.getStorage().indexIterator().toIntArray();

          int[] indices = new int[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          IntAVLTreeSet avl = new IntAVLTreeSet(indices);

          IntBidirectionalIterator iter = avl.iterator();
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

          newStorage = new IntIntSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          int[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          int size = v1.size();
          for (int i = 0; i < size; i++) {
            int idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          int[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          int[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getDoubleValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getDoubleValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getDoubleValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongDoubleSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          double[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          double[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getFloatValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getFloatValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongFloatSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          float[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          float[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVectorStorage newStorage = (LongLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getLongValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getLongValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVectorStorage newStorage = (LongLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongLongSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          long[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVectorStorage newStorage = (LongIntVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss dense storage is more efficient
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          // no rehashor one onle rehash is required, nothing to optimization
          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(v1.get(idx), entry.getIntValue()));
          }
        } else {
          // multi-rehash
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
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if (v1Size >= v2Size * Constant.sparseThreshold &&
          (v1Size + v2Size) * Constant.intersectionCoeff
              <= Constant.sparseDenseStorageThreshold * v1.dim()) {
        // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2.size(); i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
        }
      } else if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sparseDenseStorageThreshold * v1.dim()) {
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
      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        if (v1.size() + v2.size() <= 1.5 * capacity) {
          long[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < v2.size(); i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), v2Values[i]));
          }
        } else {
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
        }
      }

    } else if (v1.isSorted() && v2.isSparse()) {
      long v1Size = v1.size();
      long v2Size = v2.size();

      if ((v1Size + v2Size) * Constant.intersectionCoeff
          >= Constant.sortedDenseStorageThreshold * v1.dim()) {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] v1Indices = v1.getStorage().getIndices();
          long[] idxiter = v2.getStorage().indexIterator().toLongArray();

          long[] indices = new long[(int) (v1Size + v2Size)];
          System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());
          System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());

          LongAVLTreeSet avl = new LongAVLTreeSet(indices);

          LongBidirectionalIterator iter = avl.iterator();
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

          newStorage = new LongIntSortedVectorStorage(v1.getDim(), (int) avl.size(), indices,
              values);
        } else {
          long[] v1Indices = v1.getStorage().getIndices();
          int[] v1Values = v1.getStorage().getValues();
          long size = v1.size();
          for (int i = 0; i < size; i++) {
            long idx = v1Indices[i];
            newStorage.set(idx, v1Values[i]);
          }

          ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = entry.getLongKey();
            newStorage.set(idx, op.apply(newStorage.get(idx), entry.getIntValue()));
          }
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
      if ((size1 + size2) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1
          .dim()) {
        if (op.isKeepStorage()) {//sorted
          long[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();
          int global = 0;

          while (v1Pointor < size1 && v2Pointor < size2) {
            if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
              global++;
              v1Pointor++;
              v2Pointor++;
            } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
              resIndices[global] = v1Indices[v1Pointor];
              resValues[global] = v1Values[v1Pointor];
              global++;
              v1Pointor++;
            } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
              resIndices[global] = v2Indices[v2Pointor];
              resValues[global] = op.apply(0, v2Values[v2Pointor]);
              global++;
              v2Pointor++;
            }
          }
        } else {//dense
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      } else {
        if (op.isKeepStorage()) {
          long[] resIndices = newStorage.getIndices();
          int[] resValues = newStorage.getValues();
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
        } else {
          while (v1Pointor < size1 || v2Pointor < size2) {
            if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
              newStorage
                  .set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));
              v1Pointor++;
              v2Pointor++;
            } else if ((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
                (v1Pointor < size1 && v2Pointor >= size2)) {
              newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
              v1Pointor++;
            } else if (((v1Pointor < size1 && v2Pointor < size2)
                && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
                (v1Pointor >= size1 && v2Pointor < size2)) {
              newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));
              v2Pointor++;
            }
          }
        }
      }
    } else {
      throw new AngelException("The operation is not support!");
    }
    v1.setStorage(newStorage);

    return v1;
  }


  public static Vector apply(IntDoubleVector v1, IntDummyVector v2, Binary op) {
    IntDoubleVectorStorage newStorage = (IntDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense()) {
      double[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        double[] resValues = newStorage.getValues();

        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          newStorage.set(entry.getIntKey(), entry.getDoubleValue());
        }

        for (int idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        double[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVectorStorage newStorage = (IntFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense()) {
      float[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        float[] resValues = newStorage.getValues();

        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          newStorage.set(entry.getIntKey(), entry.getFloatValue());
        }

        for (int idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        float[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVectorStorage newStorage = (IntLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense()) {
      long[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        long[] resValues = newStorage.getValues();

        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          newStorage.set(entry.getIntKey(), entry.getLongValue());
        }

        for (int idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        long[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(IntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVectorStorage newStorage = (IntIntVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isDense()) {
      int[] v1Values = newStorage.getValues();
      int[] v2Indices = v2.getIndices();
      for (int idx : v2Indices) {
        v1Values[idx] = op.apply(v1Values[idx], 1);
      }
    } else if (v1.isSparse()) {
      int[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        int[] resValues = newStorage.getValues();

        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          newStorage.set(entry.getIntKey(), entry.getIntValue());
        }

        for (int idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        int size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          int idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        int[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVectorStorage newStorage = (LongDoubleVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        double[] resValues = newStorage.getValues();

        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          newStorage.set(entry.getLongKey(), entry.getDoubleValue());
        }

        for (long idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        long size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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
        }
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();

      long size1 = v1.size();
      long size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
          > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        double[] v1Values = v1.getStorage().getValues();

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        double[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVectorStorage newStorage = (LongFloatVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        float[] resValues = newStorage.getValues();

        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          newStorage.set(entry.getLongKey(), entry.getFloatValue());
        }

        for (long idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        long size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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
        }
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();

      long size1 = v1.size();
      long size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
          > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        float[] v1Values = v1.getStorage().getValues();

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        float[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVectorStorage newStorage = (LongLongVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        long[] resValues = newStorage.getValues();

        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          newStorage.set(entry.getLongKey(), entry.getLongValue());
        }

        for (long idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        long size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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
        }
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();

      long size1 = v1.size();
      long size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
          > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        long[] v1Values = v1.getStorage().getValues();

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        long[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

  public static Vector apply(LongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVectorStorage newStorage = (LongIntVectorStorage) StorageSwitch.apply(v1, v2, op);
    if (v1.isSparse()) {
      long[] v2Indices = v2.getIndices();

      if (((v1.size() + v2.size()) * Constant.intersectionCoeff
          > Constant.sparseDenseStorageThreshold * v1.getDim())) {
        int[] resValues = newStorage.getValues();

        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          newStorage.set(entry.getLongKey(), entry.getIntValue());
        }

        for (long idx : v2Indices) {
          newStorage.set(idx, op.apply(v1.get(idx), 1));
        }

      } else {
        // to avoid multi-rehash
        int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
        long size = v2.size();
        if (v1.size() + v2.size() < 1.5 * capacity) {
          for (int i = 0; i < size; i++) {
            long idx = v2Indices[i];
            newStorage.set(idx, op.apply(v1.get(idx), 1));
          }
        } else {
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
        }
      }
    } else { // sorted
      long[] v1Indices = v1.getStorage().getIndices();
      long[] v2Indices = v2.getIndices();

      long size1 = v1.size();
      long size2 = v2.size();
      if (!op.isKeepStorage() && ((size1 + size2) * Constant.intersectionCoeff
          > Constant.sortedDenseStorageThreshold * v1.getDim())) {
        int[] v1Values = v1.getStorage().getValues();

        for (int i = 0; i < size1; i++) {
          newStorage.set(v1Indices[i], v1Values[i]);
        }

        for (int i = 0; i < size2; i++) {
          long idx = v2Indices[i];
          newStorage.set(idx, op.apply(newStorage.get(idx), 1));
        }

      } else {
        int v1Pointor = 0;
        int v2Pointor = 0;

        int[] v1Values = v1.getStorage().getValues();

        while (v1Pointor < size1 || v2Pointor < size2) {
          if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));
            v1Pointor++;
            v2Pointor++;
          } else if ((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] < v2Indices[v2Pointor] ||
              (v1Pointor < size1 && v2Pointor >= size2)) {
            newStorage.set(v1Indices[v1Pointor], v1Values[v1Pointor]);
            v1Pointor++;
          } else if (((v1Pointor < size1 && v2Pointor < size2)
              && v1Indices[v1Pointor] >= v2Indices[v2Pointor]) ||
              (v1Pointor >= size1 && v2Pointor < size2)) {
            newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));
            v2Pointor++;
          }
        }
      }
    }
    v1.setStorage(newStorage);

    return v1;
  }

}