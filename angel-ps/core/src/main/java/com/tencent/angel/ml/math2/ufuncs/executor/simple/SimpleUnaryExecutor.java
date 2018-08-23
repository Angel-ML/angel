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
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class SimpleUnaryExecutor {
  public static Vector apply(Vector v1, Unary op) {
    if (v1 instanceof IntDoubleVector) {
      return apply((IntDoubleVector) v1, op);
    } else if (v1 instanceof IntFloatVector) {
      return apply((IntFloatVector) v1, op);
    } else if (v1 instanceof IntLongVector) {
      return apply((IntLongVector) v1, op);
    } else if (v1 instanceof IntIntVector) {
      return apply((IntIntVector) v1, op);
    } else if (v1 instanceof LongDoubleVector) {
      return apply((LongDoubleVector) v1, op);
    } else if (v1 instanceof LongFloatVector) {
      return apply((LongFloatVector) v1, op);
    } else if (v1 instanceof LongLongVector) {
      return apply((LongLongVector) v1, op);
    } else if (v1 instanceof LongIntVector) {
      return apply((LongIntVector) v1, op);
    } else {
      throw new AngelException("Vector type is not support!");
    }
  }

  private static Vector apply(IntDoubleVector v1, Unary op) {
    IntDoubleVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        double[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getDoubleValue()));
        }
      } else if (v1.isSorted()) {
        double[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      IntDoubleVectorStorage newstorage = v1.getStorage().emptyDense();
      IntDoubleVectorStorage storage = v1.getStorage();
      double[] values = newstorage.getValues();

      double tmp = op.apply((double) 0);
      int dim = v1.getDim();
      for (int i = 0; i < dim; i++) {
        values[i] = tmp;
      }

      if (v1.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          values[entry.getIntKey()] = op.apply(entry.getDoubleValue());
        }
      } else { //sort
        int[] idxs = storage.getIndices();
        double[] v1Values = storage.getValues();
        for (int k = 0; k < idxs.length; k++) {
          values[idxs[k]] = op.apply(v1Values[k]);
        }
      }

      if (op.isInplace()) {
        v1.setStorage(newstorage);
        res = v1;
      } else {
        res = new IntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newstorage);
      }
    }
    return res;
  }

  private static Vector apply(IntFloatVector v1, Unary op) {
    IntFloatVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        float[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getFloatValue()));
        }
      } else if (v1.isSorted()) {
        float[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      IntFloatVectorStorage newstorage = v1.getStorage().emptyDense();
      IntFloatVectorStorage storage = v1.getStorage();
      float[] values = newstorage.getValues();

      float tmp = op.apply((float) 0);
      int dim = v1.getDim();
      for (int i = 0; i < dim; i++) {
        values[i] = tmp;
      }

      if (v1.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          values[entry.getIntKey()] = op.apply(entry.getFloatValue());
        }
      } else { //sort
        int[] idxs = storage.getIndices();
        float[] v1Values = storage.getValues();
        for (int k = 0; k < idxs.length; k++) {
          values[idxs[k]] = op.apply(v1Values[k]);
        }
      }

      if (op.isInplace()) {
        v1.setStorage(newstorage);
        res = v1;
      } else {
        res = new IntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newstorage);
      }
    }
    return res;
  }

  private static Vector apply(IntLongVector v1, Unary op) {
    IntLongVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        long[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getLongValue()));
        }
      } else if (v1.isSorted()) {
        long[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      IntLongVectorStorage newstorage = v1.getStorage().emptyDense();
      IntLongVectorStorage storage = v1.getStorage();
      long[] values = newstorage.getValues();

      long tmp = op.apply((long) 0);
      int dim = v1.getDim();
      for (int i = 0; i < dim; i++) {
        values[i] = tmp;
      }

      if (v1.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          values[entry.getIntKey()] = op.apply(entry.getLongValue());
        }
      } else { //sort
        int[] idxs = storage.getIndices();
        long[] v1Values = storage.getValues();
        for (int k = 0; k < idxs.length; k++) {
          values[idxs[k]] = op.apply(v1Values[k]);
        }
      }

      if (op.isInplace()) {
        v1.setStorage(newstorage);
        res = v1;
      } else {
        res = new IntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          newstorage);
      }
    }
    return res;
  }

  private static Vector apply(IntIntVector v1, Unary op) {
    IntIntVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        int[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getIntValue()));
        }
      } else if (v1.isSorted()) {
        int[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      IntIntVectorStorage newstorage = v1.getStorage().emptyDense();
      IntIntVectorStorage storage = v1.getStorage();
      int[] values = newstorage.getValues();

      int tmp = op.apply((int) 0);
      int dim = v1.getDim();
      for (int i = 0; i < dim; i++) {
        values[i] = tmp;
      }

      if (v1.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          values[entry.getIntKey()] = op.apply(entry.getIntValue());
        }
      } else { //sort
        int[] idxs = storage.getIndices();
        int[] v1Values = storage.getValues();
        for (int k = 0; k < idxs.length; k++) {
          values[idxs[k]] = op.apply(v1Values[k]);
        }
      }

      if (op.isInplace()) {
        v1.setStorage(newstorage);
        res = v1;
      } else {
        res =
          new IntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newstorage);
      }
    }
    return res;
  }

  private static Vector apply(LongDoubleVector v1, Unary op) {
    LongDoubleVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        double[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getDoubleValue()));
        }
      } else if (v1.isSorted()) {
        double[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      throw new AngelException("The operation is not supported!");
    }
    return res;
  }

  private static Vector apply(LongFloatVector v1, Unary op) {
    LongFloatVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        float[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Long2FloatMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getFloatValue()));
        }
      } else if (v1.isSorted()) {
        float[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      throw new AngelException("The operation is not supported!");
    }
    return res;
  }

  private static Vector apply(LongLongVector v1, Unary op) {
    LongLongVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        long[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Long2LongMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getLongValue()));
        }
      } else if (v1.isSorted()) {
        long[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      throw new AngelException("The operation is not supported!");
    }
    return res;
  }

  private static Vector apply(LongIntVector v1, Unary op) {
    LongIntVector res;
    if (op.isOrigin() || v1.isDense()) {
      if (!op.isInplace()) {
        res = v1.copy();
      } else {
        res = v1;
      }
      if (v1.isDense()) {
        int[] values = res.getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = op.apply(values[i]);
        }
      } else if (v1.isSparse()) {
        ObjectIterator<Long2IntMap.Entry> iter = res.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          entry.setValue(op.apply(entry.getIntValue()));
        }
      } else if (v1.isSorted()) {
        int[] values = res.getStorage().getValues();
        for (int i = 0; i < v1.size(); i++) {
          values[i] = op.apply(values[i]);
        }
      } else {
        throw new AngelException("The operation is not support!");
      }
    } else {
      throw new AngelException("The operation is not supported!");
    }
    return res;
  }

}
