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


public class SimpleBinaryOutAllExecutor {
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
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getDoubleValue());
        }
      } else {
        IntDoubleVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntDoubleVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getDoubleValue());
            }
          }
        } else {
          IntDoubleVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          double[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntDoubleVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getDoubleValue());
            }
          }
        } else {
          IntDoubleVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getFloatValue());
        }
      } else {
        IntFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getFloatValue());
            }
          }
        } else {
          IntFloatVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntFloatVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getFloatValue());
            }
          }
        } else {
          IntFloatVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
        }
      } else {
        IntLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getFloatValue());
        }
      } else {
        IntFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getFloatValue());
            }
          }
        } else {
          IntFloatVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          float[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntFloatVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getFloatValue());
            }
          }
        } else {
          IntFloatVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Float.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
        }
      } else {
        IntLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Float.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Float.NaN;
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
        }
      } else {
        IntLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          long[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getLongValue());
            }
          }
        } else {
          IntLongVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        if (size1 != v1.getDim() && size2 != v2.getDim()) {
          for (int i = 0; i < v1.getDim(); i++) {
            resValues[i] = 0 / 0;
          }
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
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

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        if (size1 != v1.getDim() && size2 != v2.getDim()) {
          for (int i = 0; i < v1.getDim(); i++) {
            resValues[i] = 0 / 0;
          }
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(IntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVector res;
    if (v1.isDense() && v2.isDense()) {
      res = v1.copy();
      int[] resValues = res.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      for (int idx = 0; idx < resValues.length; idx++) {
        resValues[idx] = op.apply(resValues[idx], v2Values[idx]);
      }
    } else if (v1.isDense() && v2.isSparse()) {
      res = v1.copy();
      int[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
      }
    } else if (v1.isDense() && v2.isSorted()) {
      res = v1.copy();
      int[] resValues = res.getStorage().getValues();
      if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = op.apply(resValues[i], 0);
        }

        int size = v2.size();
        for (int i = 0; i < size; i++) {
          int idx = v2Indices[i];
          resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
        }
      } else {
        IntIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < resValues.length; i++) {
          if (v2Storage.hasKey(i)) {
            resValues[i] = op.apply(resValues[i], v2.get(i));
          } else {
            resValues[i] = op.apply(resValues[i], 0);
          }
        }
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

        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
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

        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2IntMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getIntValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2IntMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getIntValue();
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          int[] v2Indices = v2.getStorage().getIndices();
          int[] v2Values = v2.getStorage().getValues();
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }

          int size = v2.size();
          for (int i = 0; i < size; i++) {
            int idx = v2Indices[i];
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), v2Values[i]);
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        if (v2.size() < Constant.denseLoopThreshold * v2.getDim()) {
          for (int i = 0; i < resValues.length; i++) {
            resValues[i] = op.apply(resValues[i], 0);
          }
          ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            if (v1.getStorage().hasKey(idx)) {
              resValues[idx] = op.apply(v1.get(idx), entry.getIntValue());
            }
          }
        } else {
          IntIntVectorStorage v2Storage = v2.getStorage();
          for (int i = 0; i < resValues.length; i++) {
            if (v2Storage.hasKey(i)) {
              resValues[i] = op.apply(resValues[i], v2.get(i));
            } else {
              resValues[i] = op.apply(resValues[i], 0);
            }
          }
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        int size1 = v1.size();
        int size2 = v2.size();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        if (size1 != v1.getDim() && size2 != v2.getDim()) {
          for (int i = 0; i < v1.getDim(); i++) {
            resValues[i] = 0 / 0;
          }
        }
        while (v1Pointor < size1 && v2Pointor < size2) {
          if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);
            v1Pointor++;
            v2Pointor++;
          } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {
            resValues[v1Indices[v1Pointor]] = op.apply(v1Values[v1Pointor], 0);
            v1Pointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resValues[v2Indices[v2Pointor]] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
          }
        }

        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongDoubleVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongDoubleVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = new LongDoubleSparseVectorStorage(v1.getDim());

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongDoubleVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = new LongDoubleSparseVectorStorage(v1.getDim());

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = new LongDoubleSparseVectorStorage(v1.getDim());

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = new LongDoubleSparseVectorStorage(v1.getDim());

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, 0));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        double[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Double.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = new LongFloatSparseVectorStorage(v1.getDim());

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongFloatVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Float.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = new LongFloatSparseVectorStorage(v1.getDim());

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Float.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = new LongFloatSparseVectorStorage(v1.getDim());

        LongFloatVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, 0));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        float[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        for (int i = 0; i < resValues.length; i++) {
          resValues[i] = Float.NaN;
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongLongVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongLongVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = new LongLongSparseVectorStorage(v1.getDim());

        LongLongVectorStorage v1Storage = v1.getStorage();
        LongLongVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        long[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();

        if (size1 != v1.getDim() && size2 != v2.getDim()) {
          for (int i = 0; i < v1.getDim(); i++) {
            resValues[i] = 0 / 0;
          }
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongLongVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongLongVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = new LongLongSparseVectorStorage(v1.getDim());

        LongLongVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        long[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        if (size1 != v1.getDim() && size2 != v2.getDim()) {
          for (int i = 0; i < v1.getDim(); i++) {
            resValues[i] = 0 / 0;
          }
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }

  public static Vector apply(LongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVector res;
    if (v1.isSparse() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongIntVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongIntVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongIntVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongIntVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongIntVectorStorage newStorage = new LongIntSparseVectorStorage(v1.getDim());

        LongIntVectorStorage v1Storage = v1.getStorage();
        LongIntVectorStorage v2Storage = v2.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else if (v1Storage.hasKey(i) && !v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), 0));
          } else if (!v1Storage.hasKey(i) && v2Storage.hasKey(i)) {
            newStorage.set(i, op.apply(0, v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, 0));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongIntVectorStorage newStorage = v1.getStorage().emptySorted((int) (v1.getDim()));
        long[] resIndices = newStorage.getIndices();
        int[] resValues = newStorage.getValues();

        int v1Pointor = 0;
        int v2Pointor = 0;
        long size1 = v1.size();
        long size2 = v2.size();

        long[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();

        if (size1 != v1.getDim() && size2 != v2.getDim()) {
          for (int i = 0; i < v1.getDim(); i++) {
            resValues[i] = 0 / 0;
          }
        }

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
            resValues[globalPointor] = op.apply(v2Values[v2Pointor], 0);
            v1Pointor++;
            globalPointor++;
          } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]
            resIndices[globalPointor] = v2Indices[v2Pointor];
            resValues[globalPointor] = op.apply(0, v2Values[v2Pointor]);
            v2Pointor++;
            globalPointor++;
          }
        }

        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else {
      throw new AngelException("The operation is not support!");
    }

    return res;
  }


  public static Vector apply(IntDoubleVector v1, IntDummyVector v2, Binary op) {
    IntDoubleVector res;

    if (v1.isDense()) {
      res = v1.copy();
      double[] resValues = res.getStorage().getValues();
      for (int i = 0; i < v1.getDim(); i++) {
        resValues[i] = op.apply(resValues[i], v2.get(i));
      }
    } else if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();
        ObjectIterator<Int2DoubleMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2DoubleMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getDoubleValue();
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntDoubleVectorStorage newStorage = v1.getStorage().emptyDense();
        double[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(IntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVector res;

    if (v1.isDense()) {
      res = v1.copy();
      float[] resValues = res.getStorage().getValues();
      for (int i = 0; i < v1.getDim(); i++) {
        resValues[i] = op.apply(resValues[i], v2.get(i));
      }
    } else if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();
        ObjectIterator<Int2FloatMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2FloatMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getFloatValue();
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntFloatVectorStorage newStorage = v1.getStorage().emptyDense();
        float[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(IntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVector res;

    if (v1.isDense()) {
      res = v1.copy();
      long[] resValues = res.getStorage().getValues();
      for (int i = 0; i < v1.getDim(); i++) {
        resValues[i] = op.apply(resValues[i], v2.get(i));
      }
    } else if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();
        ObjectIterator<Int2LongMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2LongMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getLongValue();
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntLongVectorStorage newStorage = v1.getStorage().emptyDense();
        long[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(IntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVector res;

    if (v1.isDense()) {
      res = v1.copy();
      int[] resValues = res.getStorage().getValues();
      for (int i = 0; i < v1.getDim(); i++) {
        resValues[i] = op.apply(resValues[i], v2.get(i));
      }
    } else if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();
        ObjectIterator<Int2IntMap.Entry> iter1 = v1.getStorage().entryIterator();
        while (iter1.hasNext()) {
          Int2IntMap.Entry entry = iter1.next();
          int idx = entry.getIntKey();
          resValues[idx] = entry.getIntValue();
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        IntIntVectorStorage newStorage = v1.getStorage().emptyDense();
        int[] resValues = newStorage.getValues();

        int[] v1Indices = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          int idx = v1Indices[i];
          resValues[idx] = v1Values[i];
        }

        for (int i = 0; i < v1.getDim(); i++) {
          resValues[i] = op.apply(resValues[i], v2.get(i));
        }
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVector res;

    if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongDoubleVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, v2.get(i)));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongDoubleVectorStorage newStorage = new LongDoubleSparseVectorStorage(v1.getDim());

        LongDoubleVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0, v2.get(i)));
          }
        }
        res = new LongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVector res;

    if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongFloatVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongFloatVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, v2.get(i)));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongFloatVectorStorage newStorage = new LongFloatSparseVectorStorage(v1.getDim());

        LongFloatVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0.0f, v2.get(i)));
          }
        }
        res = new LongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVector res;

    if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongLongVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongLongVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, v2.get(i)));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongLongVectorStorage newStorage = new LongLongSparseVectorStorage(v1.getDim());

        LongLongVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, v2.get(i)));
          }
        }
        res = new LongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }

  public static Vector apply(LongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVector res;

    if (v1.isSparse()) {
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        // multi-rehash
        LongIntVectorStorage newStorage = v1.getStorage().emptySparse((int) (v1.getDim()));

        LongIntVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, v2.get(i)));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    } else { // sorted
      if (op.isKeepStorage()) {
        throw new AngelException("operation is not support!");
      } else {
        LongIntVectorStorage newStorage = new LongIntSparseVectorStorage(v1.getDim());

        LongIntVectorStorage v1Storage = v1.getStorage();
        for (int i = 0; i < v1.getDim(); i++) {
          if (v1Storage.hasKey(i)) {
            newStorage.set(i, op.apply(v1.get(i), v2.get(i)));
          } else {
            newStorage.set(i, op.apply(0, v2.get(i)));
          }
        }
        res = new LongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newStorage);
      }
    }

    return res;
  }
}