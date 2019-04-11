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


package com.tencent.angel.ml.math2.utils;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.CompIntDoubleVector;
import com.tencent.angel.ml.math2.vector.CompIntFloatVector;
import com.tencent.angel.ml.math2.vector.CompIntIntVector;
import com.tencent.angel.ml.math2.vector.CompIntLongVector;
import com.tencent.angel.ml.math2.vector.CompLongDoubleVector;
import com.tencent.angel.ml.math2.vector.CompLongFloatVector;
import com.tencent.angel.ml.math2.vector.CompLongIntVector;
import com.tencent.angel.ml.math2.vector.CompLongLongVector;
import com.tencent.angel.ml.math2.vector.ComponentVector;
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
import com.tencent.angel.ml.math2.vector.SimpleVector;
import com.tencent.angel.ml.math2.vector.Vector;

public class VectorUtils {

  private static ComponentVector emptyLike(ComponentVector v) {
    ComponentVector result;
    if (v instanceof CompIntDoubleVector) {
      IntDoubleVector[] parts = new IntDoubleVector[v.getNumPartitions()];
      IntDoubleVector[] refParts = ((CompIntDoubleVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (IntDoubleVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompIntDoubleVector(((CompIntDoubleVector) v).getMatrixId(),
          ((CompIntDoubleVector) v).getRowId(), ((CompIntDoubleVector) v).getClock(),
          ((CompIntDoubleVector) v).getDim(), parts, ((CompIntDoubleVector) v).getSubDim());
    } else if (v instanceof CompIntFloatVector) {
      IntFloatVector[] parts = new IntFloatVector[v.getNumPartitions()];
      IntFloatVector[] refParts = ((CompIntFloatVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (IntFloatVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompIntFloatVector(((CompIntFloatVector) v).getMatrixId(),
          ((CompIntFloatVector) v).getRowId(), ((CompIntFloatVector) v).getClock(),
          ((CompIntFloatVector) v).getDim(), parts, ((CompIntFloatVector) v).getSubDim());
    } else if (v instanceof CompIntLongVector) {
      IntLongVector[] parts = new IntLongVector[v.getNumPartitions()];
      IntLongVector[] refParts = ((CompIntLongVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (IntLongVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompIntLongVector(((CompIntLongVector) v).getMatrixId(),
          ((CompIntLongVector) v).getRowId(), ((CompIntLongVector) v).getClock(),
          ((CompIntLongVector) v).getDim(), parts, ((CompIntLongVector) v).getSubDim());
    } else if (v instanceof CompIntIntVector) {
      IntIntVector[] parts = new IntIntVector[v.getNumPartitions()];
      IntIntVector[] refParts = ((CompIntIntVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (IntIntVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompIntIntVector(((CompIntIntVector) v).getMatrixId(),
          ((CompIntIntVector) v).getRowId(), ((CompIntIntVector) v).getClock(),
          ((CompIntIntVector) v).getDim(), parts, ((CompIntIntVector) v).getSubDim());
    } else if (v instanceof CompLongDoubleVector) {
      LongDoubleVector[] parts = new LongDoubleVector[v.getNumPartitions()];
      LongDoubleVector[] refParts = ((CompLongDoubleVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (LongDoubleVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompLongDoubleVector(((CompLongDoubleVector) v).getMatrixId(),
          ((CompLongDoubleVector) v).getRowId(), ((CompLongDoubleVector) v).getClock(),
          ((CompLongDoubleVector) v).getDim(), parts, ((CompLongDoubleVector) v).getSubDim());
    } else if (v instanceof CompLongFloatVector) {
      LongFloatVector[] parts = new LongFloatVector[v.getNumPartitions()];
      LongFloatVector[] refParts = ((CompLongFloatVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (LongFloatVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompLongFloatVector(((CompLongFloatVector) v).getMatrixId(),
          ((CompLongFloatVector) v).getRowId(), ((CompLongFloatVector) v).getClock(),
          ((CompLongFloatVector) v).getDim(), parts, ((CompLongFloatVector) v).getSubDim());
    } else if (v instanceof CompLongLongVector) {
      LongLongVector[] parts = new LongLongVector[v.getNumPartitions()];
      LongLongVector[] refParts = ((CompLongLongVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (LongLongVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompLongLongVector(((CompLongLongVector) v).getMatrixId(),
          ((CompLongLongVector) v).getRowId(), ((CompLongLongVector) v).getClock(),
          ((CompLongLongVector) v).getDim(), parts, ((CompLongLongVector) v).getSubDim());
    } else if (v instanceof CompLongIntVector) {
      LongIntVector[] parts = new LongIntVector[v.getNumPartitions()];
      LongIntVector[] refParts = ((CompLongIntVector) v).getPartitions();
      for (int i = 0; i < refParts.length; i++) {
        if (null != refParts[i]) {
          parts[i] = (LongIntVector) emptyLike((SimpleVector) refParts[i]);
        }
      }
      result = new CompLongIntVector(((CompLongIntVector) v).getMatrixId(),
          ((CompLongIntVector) v).getRowId(), ((CompLongIntVector) v).getClock(),
          ((CompLongIntVector) v).getDim(), parts, ((CompLongIntVector) v).getSubDim());
    } else {
      throw new AngelException("The operation is not support!");
    }

    return result;
  }

  private static SimpleVector emptyLike(SimpleVector v) {
    if (v instanceof IntDoubleVector) {
      if (((IntDoubleVector) v).isDense()) {
        return VFactory
            .denseDoubleVector(((IntDoubleVector) v).getMatrixId(),
                ((IntDoubleVector) v).getRowId(),
                ((IntDoubleVector) v).getClock(), ((IntDoubleVector) v).getDim());
      } else if (((IntDoubleVector) v).isSparse()) {
        int capacity = (int) (((IntDoubleVector) v).size() * 1.25);
        return VFactory
            .sparseDoubleVector(((IntDoubleVector) v).getMatrixId(),
                ((IntDoubleVector) v).getRowId(),
                ((IntDoubleVector) v).getClock(), ((IntDoubleVector) v).getDim(), capacity);
      } else {
        return VFactory
            .sortedDoubleVector(((IntDoubleVector) v).getMatrixId(),
                ((IntDoubleVector) v).getRowId(),
                ((IntDoubleVector) v).getClock(), ((IntDoubleVector) v).getDim(),
                ((IntDoubleVector) v).size());
      }
    } else if (v instanceof IntFloatVector) {
      if (((IntFloatVector) v).isDense()) {
        return VFactory
            .denseFloatVector(((IntFloatVector) v).getMatrixId(), ((IntFloatVector) v).getRowId(),
                ((IntFloatVector) v).getClock(), ((IntFloatVector) v).getDim());
      } else if (((IntFloatVector) v).isSparse()) {
        int capacity = (int) (((IntFloatVector) v).size() * 1.25);
        return VFactory
            .sparseFloatVector(((IntFloatVector) v).getMatrixId(), ((IntFloatVector) v).getRowId(),
                ((IntFloatVector) v).getClock(), ((IntFloatVector) v).getDim(), capacity);
      } else {
        return VFactory
            .sortedFloatVector(((IntFloatVector) v).getMatrixId(), ((IntFloatVector) v).getRowId(),
                ((IntFloatVector) v).getClock(), ((IntFloatVector) v).getDim(),
                ((IntFloatVector) v).size());
      }
    } else if (v instanceof IntLongVector) {
      if (((IntLongVector) v).isDense()) {
        return VFactory
            .denseLongVector(((IntLongVector) v).getMatrixId(), ((IntLongVector) v).getRowId(),
                ((IntLongVector) v).getClock(), ((IntLongVector) v).getDim());
      } else if (((IntLongVector) v).isSparse()) {
        int capacity = (int) (((IntLongVector) v).size() * 1.25);
        return VFactory
            .sparseLongVector(((IntLongVector) v).getMatrixId(), ((IntLongVector) v).getRowId(),
                ((IntLongVector) v).getClock(), ((IntLongVector) v).getDim(), capacity);
      } else {
        return VFactory
            .sortedLongVector(((IntLongVector) v).getMatrixId(), ((IntLongVector) v).getRowId(),
                ((IntLongVector) v).getClock(), ((IntLongVector) v).getDim(),
                ((IntLongVector) v).size());
      }
    } else if (v instanceof IntIntVector) {
      if (((IntIntVector) v).isDense()) {
        return VFactory
            .denseIntVector(((IntIntVector) v).getMatrixId(), ((IntIntVector) v).getRowId(),
                ((IntIntVector) v).getClock(), ((IntIntVector) v).getDim());
      } else if (((IntIntVector) v).isSparse()) {
        int capacity = (int) (((IntIntVector) v).size() * 1.25);
        return VFactory
            .sparseIntVector(((IntIntVector) v).getMatrixId(), ((IntIntVector) v).getRowId(),
                ((IntIntVector) v).getClock(), ((IntIntVector) v).getDim(), capacity);
      } else {
        return VFactory
            .sortedIntVector(((IntIntVector) v).getMatrixId(), ((IntIntVector) v).getRowId(),
                ((IntIntVector) v).getClock(), ((IntIntVector) v).getDim(),
                ((IntIntVector) v).size());
      }
    } else if (v instanceof LongDoubleVector) {
      if (((LongDoubleVector) v).isSparse()) {
        int capacity = (int) (((LongDoubleVector) v).size() * 1.25);
        return VFactory.sparseLongKeyDoubleVector(((LongDoubleVector) v).getMatrixId(),
            ((LongDoubleVector) v).getRowId(), ((LongDoubleVector) v).getClock(),
            ((LongDoubleVector) v).getDim(), capacity);
      } else if (((LongDoubleVector) v).isSorted()) {
        return VFactory.sortedLongKeyDoubleVector(((LongDoubleVector) v).getMatrixId(),
            ((LongDoubleVector) v).getRowId(), ((LongDoubleVector) v).getClock(),
            ((LongDoubleVector) v).getDim(), (int) ((LongDoubleVector) v).size());
      } else {
        throw new AngelException("LongKey dense is not support in emptyLike!");
      }
    } else if (v instanceof LongFloatVector) {
      if (((LongFloatVector) v).isSparse()) {
        int capacity = (int) (((LongFloatVector) v).size() * 1.25);
        return VFactory.sparseLongKeyFloatVector(((LongFloatVector) v).getMatrixId(),
            ((LongFloatVector) v).getRowId(), ((LongFloatVector) v).getClock(),
            ((LongFloatVector) v).getDim(), capacity);
      } else if (((LongFloatVector) v).isSorted()) {
        return VFactory.sortedLongKeyFloatVector(((LongFloatVector) v).getMatrixId(),
            ((LongFloatVector) v).getRowId(), ((LongFloatVector) v).getClock(),
            ((LongFloatVector) v).getDim(), (int) ((LongFloatVector) v).size());
      } else {
        throw new AngelException("LongKey dense is not support in emptyLike!");
      }
    } else if (v instanceof LongLongVector) {
      if (((LongLongVector) v).isSparse()) {
        int capacity = (int) (((LongLongVector) v).size() * 1.25);
        return VFactory.sparseLongKeyLongVector(((LongLongVector) v).getMatrixId(),
            ((LongLongVector) v).getRowId(), ((LongLongVector) v).getClock(),
            ((LongLongVector) v).getDim(), capacity);
      } else if (((LongLongVector) v).isSorted()) {
        return VFactory.sortedLongKeyLongVector(((LongLongVector) v).getMatrixId(),
            ((LongLongVector) v).getRowId(), ((LongLongVector) v).getClock(),
            ((LongLongVector) v).getDim(), (int) ((LongLongVector) v).size());
      } else {
        throw new AngelException("LongKey dense is not support in emptyLike!");
      }
    } else if (v instanceof LongIntVector) {
      if (((LongIntVector) v).isSparse()) {
        int capacity = (int) (((LongIntVector) v).size() * 1.25);
        return VFactory
            .sparseLongKeyIntVector(((LongIntVector) v).getMatrixId(),
                ((LongIntVector) v).getRowId(),
                ((LongIntVector) v).getClock(), ((LongIntVector) v).getDim(), capacity);
      } else if (((LongIntVector) v).isSorted()) {
        return VFactory
            .sortedLongKeyIntVector(((LongIntVector) v).getMatrixId(),
                ((LongIntVector) v).getRowId(),
                ((LongIntVector) v).getClock(), ((LongIntVector) v).getDim(),
                (int) ((LongIntVector) v).size());
      } else {
        throw new AngelException("LongKey dense is not support in emptyLike!");
      }
    } else {
      throw new AngelException("Dummy is not support in emptyLike!");
    }
  }

  public static Vector emptyLike(Vector v) {
    if (v instanceof ComponentVector) {
      return (Vector) emptyLike((ComponentVector) v);
    } else {
      return (Vector) emptyLike((SimpleVector) v);
    }
  }

  public static double getDouble(Vector v, long idx) {
    if (v instanceof CompIntDoubleVector) {
      return ((CompIntDoubleVector) v).get((int) idx);
    } else if (v instanceof IntDoubleVector) {
      return ((IntDoubleVector) v).get((int) idx);
    } else if (v instanceof CompLongDoubleVector) {
      return ((CompLongDoubleVector) v).get(idx);
    } else if (v instanceof LongDoubleVector) {
      return ((LongDoubleVector) v).get((int) idx);
    } else if (v instanceof CompIntFloatVector) {
      return ((CompIntFloatVector) v).get((int) idx);
    } else if (v instanceof IntFloatVector) {
      return ((IntFloatVector) v).get((int) idx);
    } else if (v instanceof CompLongFloatVector) {
      return ((CompLongFloatVector) v).get(idx);
    } else if (v instanceof LongFloatVector) {
      return ((LongFloatVector) v).get((int) idx);
    } else if (v instanceof CompIntLongVector) {
      return ((CompIntLongVector) v).get((int) idx);
    } else if (v instanceof IntLongVector) {
      return ((IntLongVector) v).get((int) idx);
    } else if (v instanceof CompLongLongVector) {
      return ((CompLongLongVector) v).get(idx);
    } else if (v instanceof LongLongVector) {
      return ((LongLongVector) v).get((int) idx);
    } else if (v instanceof CompIntIntVector) {
      return ((CompIntIntVector) v).get((int) idx);
    } else if (v instanceof IntIntVector) {
      return ((IntIntVector) v).get((int) idx);
    } else if (v instanceof CompLongIntVector) {
      return ((CompLongIntVector) v).get(idx);
    } else if (v instanceof LongIntVector) {
      return ((LongIntVector) v).get((int) idx);
    } else if (v instanceof IntDummyVector) {
      return ((IntDummyVector) v).get((int) idx);
    } else if (v instanceof LongDummyVector) {
      return ((LongDummyVector) v).get((int) idx);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static float getFloat(Vector v, long idx) {
    if (v instanceof CompIntFloatVector) {
      return ((CompIntFloatVector) v).get((int) idx);
    } else if (v instanceof IntFloatVector) {
      return ((IntFloatVector) v).get((int) idx);
    } else if (v instanceof CompLongFloatVector) {
      return ((CompLongFloatVector) v).get(idx);
    } else if (v instanceof LongFloatVector) {
      return ((LongFloatVector) v).get((int) idx);
    } else if (v instanceof CompIntLongVector) {
      return ((CompIntLongVector) v).get((int) idx);
    } else if (v instanceof IntLongVector) {
      return ((IntLongVector) v).get((int) idx);
    } else if (v instanceof CompLongLongVector) {
      return ((CompLongLongVector) v).get(idx);
    } else if (v instanceof LongLongVector) {
      return ((LongLongVector) v).get((int) idx);
    } else if (v instanceof CompIntIntVector) {
      return ((CompIntIntVector) v).get((int) idx);
    } else if (v instanceof IntIntVector) {
      return ((IntIntVector) v).get((int) idx);
    } else if (v instanceof CompLongIntVector) {
      return ((CompLongIntVector) v).get(idx);
    } else if (v instanceof LongIntVector) {
      return ((LongIntVector) v).get((int) idx);
    } else if (v instanceof IntDummyVector) {
      return ((IntDummyVector) v).get((int) idx);
    } else if (v instanceof LongDummyVector) {
      return ((LongDummyVector) v).get((int) idx);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static long getLong(Vector v, long idx) {
    if (v instanceof CompIntLongVector) {
      return ((CompIntLongVector) v).get((int) idx);
    } else if (v instanceof IntLongVector) {
      return ((IntLongVector) v).get((int) idx);
    } else if (v instanceof CompLongLongVector) {
      return ((CompLongLongVector) v).get(idx);
    } else if (v instanceof LongLongVector) {
      return ((LongLongVector) v).get((int) idx);
    } else if (v instanceof CompIntIntVector) {
      return ((CompIntIntVector) v).get((int) idx);
    } else if (v instanceof IntIntVector) {
      return ((IntIntVector) v).get((int) idx);
    } else if (v instanceof CompLongIntVector) {
      return ((CompLongIntVector) v).get(idx);
    } else if (v instanceof LongIntVector) {
      return ((LongIntVector) v).get((int) idx);
    } else if (v instanceof IntDummyVector) {
      return ((IntDummyVector) v).get((int) idx);
    } else if (v instanceof LongDummyVector) {
      return ((LongDummyVector) v).get((int) idx);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static int getInt(Vector v, long idx) {
    if (v instanceof CompIntIntVector) {
      return ((CompIntIntVector) v).get((int) idx);
    } else if (v instanceof IntIntVector) {
      return ((IntIntVector) v).get((int) idx);
    } else if (v instanceof CompLongIntVector) {
      return ((CompLongIntVector) v).get(idx);
    } else if (v instanceof LongIntVector) {
      return ((LongIntVector) v).get((int) idx);
    } else if (v instanceof IntDummyVector) {
      return ((IntDummyVector) v).get((int) idx);
    } else if (v instanceof LongDummyVector) {
      return ((LongDummyVector) v).get((int) idx);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static void setInt(Vector v, long idx, int value) {
    if (v instanceof CompIntDoubleVector) {
      ((CompIntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof IntDoubleVector) {
      ((IntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompLongDoubleVector) {
      ((CompLongDoubleVector) v).set(idx, value);
    } else if (v instanceof LongDoubleVector) {
      ((LongDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompIntFloatVector) {
      ((CompIntFloatVector) v).set((int) idx, value);
    } else if (v instanceof IntFloatVector) {
      ((IntFloatVector) v).set((int) idx, value);
    } else if (v instanceof CompLongFloatVector) {
      ((CompLongFloatVector) v).set(idx, value);
    } else if (v instanceof LongFloatVector) {
      ((LongFloatVector) v).set((int) idx, value);
    } else if (v instanceof CompIntLongVector) {
      ((CompIntLongVector) v).set((int) idx, value);
    } else if (v instanceof IntLongVector) {
      ((IntLongVector) v).set((int) idx, value);
    } else if (v instanceof CompLongLongVector) {
      ((CompLongLongVector) v).set(idx, value);
    } else if (v instanceof LongLongVector) {
      ((LongLongVector) v).set((int) idx, value);
    } else if (v instanceof CompIntIntVector) {
      ((CompIntIntVector) v).set((int) idx, value);
    } else if (v instanceof IntIntVector) {
      ((IntIntVector) v).set((int) idx, value);
    } else if (v instanceof CompLongIntVector) {
      ((CompLongIntVector) v).set(idx, value);
    } else if (v instanceof LongIntVector) {
      ((LongIntVector) v).set((int) idx, value);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static void setLong(Vector v, long idx, long value) {
    if (v instanceof CompIntDoubleVector) {
      ((CompIntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof IntDoubleVector) {
      ((IntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompLongDoubleVector) {
      ((CompLongDoubleVector) v).set(idx, value);
    } else if (v instanceof LongDoubleVector) {
      ((LongDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompIntFloatVector) {
      ((CompIntFloatVector) v).set((int) idx, value);
    } else if (v instanceof IntFloatVector) {
      ((IntFloatVector) v).set((int) idx, value);
    } else if (v instanceof CompLongFloatVector) {
      ((CompLongFloatVector) v).set(idx, value);
    } else if (v instanceof LongFloatVector) {
      ((LongFloatVector) v).set((int) idx, value);
    } else if (v instanceof CompIntLongVector) {
      ((CompIntLongVector) v).set((int) idx, value);
    } else if (v instanceof IntLongVector) {
      ((IntLongVector) v).set((int) idx, value);
    } else if (v instanceof CompLongLongVector) {
      ((CompLongLongVector) v).set(idx, value);
    } else if (v instanceof LongLongVector) {
      ((LongLongVector) v).set((int) idx, value);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static void setFloat(Vector v, long idx, float value) {
    if (v instanceof CompIntDoubleVector) {
      ((CompIntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof IntDoubleVector) {
      ((IntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompLongDoubleVector) {
      ((CompLongDoubleVector) v).set(idx, value);
    } else if (v instanceof LongDoubleVector) {
      ((LongDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompIntFloatVector) {
      ((CompIntFloatVector) v).set((int) idx, value);
    } else if (v instanceof IntFloatVector) {
      ((IntFloatVector) v).set((int) idx, value);
    } else if (v instanceof CompLongFloatVector) {
      ((CompLongFloatVector) v).set(idx, value);
    } else if (v instanceof LongFloatVector) {
      ((LongFloatVector) v).set((int) idx, value);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }

  public static void setDouble(Vector v, long idx, double value) {
    if (v instanceof CompIntDoubleVector) {
      ((CompIntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof IntDoubleVector) {
      ((IntDoubleVector) v).set((int) idx, value);
    } else if (v instanceof CompLongDoubleVector) {
      ((CompLongDoubleVector) v).set(idx, value);
    } else if (v instanceof LongDoubleVector) {
      ((LongDoubleVector) v).set((int) idx, value);
    } else {
      throw new AngelException("Vector is not validate!");
    }
  }
}