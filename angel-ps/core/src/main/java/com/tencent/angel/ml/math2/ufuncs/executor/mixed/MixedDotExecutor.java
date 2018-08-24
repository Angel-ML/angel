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


package com.tencent.angel.ml.math2.ufuncs.executor.mixed;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class MixedDotExecutor {
  public static double apply(ComponentVector v1, Vector v2) {
    if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDoubleVector) {
      return apply((CompIntDoubleVector) v1, (IntDoubleVector) v2);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntFloatVector) {
      return apply((CompIntDoubleVector) v1, (IntFloatVector) v2);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntLongVector) {
      return apply((CompIntDoubleVector) v1, (IntLongVector) v2);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntIntVector) {
      return apply((CompIntDoubleVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDummyVector) {
      return apply((CompIntDoubleVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntFloatVector) {
      return apply((CompIntFloatVector) v1, (IntFloatVector) v2);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntLongVector) {
      return apply((CompIntFloatVector) v1, (IntLongVector) v2);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntIntVector) {
      return apply((CompIntFloatVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntDummyVector) {
      return apply((CompIntFloatVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntLongVector) {
      return apply((CompIntLongVector) v1, (IntLongVector) v2);
    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntIntVector) {
      return apply((CompIntLongVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntDummyVector) {
      return apply((CompIntLongVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof CompIntIntVector && v2 instanceof IntIntVector) {
      return apply((CompIntIntVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof CompIntIntVector && v2 instanceof IntDummyVector) {
      return apply((CompIntIntVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDoubleVector) {
      return apply((CompLongDoubleVector) v1, (LongDoubleVector) v2);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongFloatVector) {
      return apply((CompLongDoubleVector) v1, (LongFloatVector) v2);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongLongVector) {
      return apply((CompLongDoubleVector) v1, (LongLongVector) v2);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongIntVector) {
      return apply((CompLongDoubleVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDummyVector) {
      return apply((CompLongDoubleVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongFloatVector) {
      return apply((CompLongFloatVector) v1, (LongFloatVector) v2);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongLongVector) {
      return apply((CompLongFloatVector) v1, (LongLongVector) v2);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongIntVector) {
      return apply((CompLongFloatVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongDummyVector) {
      return apply((CompLongFloatVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongLongVector) {
      return apply((CompLongLongVector) v1, (LongLongVector) v2);
    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongIntVector) {
      return apply((CompLongLongVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongDummyVector) {
      return apply((CompLongLongVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof CompLongIntVector && v2 instanceof LongIntVector) {
      return apply((CompLongIntVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof CompLongIntVector && v2 instanceof LongDummyVector) {
      return apply((CompLongIntVector) v1, (LongDummyVector) v2);
    } else {
      throw new AngelException("The operation is not support!");
    }
  }

  private static double apply(CompIntDoubleVector v1, IntDummyVector v2) {
    double dotValue = 0.0;
    int[] v2Indices = v2.getIndices();
    for (int idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompIntDoubleVector v1, IntDoubleVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      double[] v2Values = v2.getStorage().getValues();
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getDoubleValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntDoubleVector v1, IntFloatVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      float[] v2Values = v2.getStorage().getValues();
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getFloatValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntDoubleVector v1, IntLongVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getLongValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntDoubleVector v1, IntIntVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntFloatVector v1, IntDummyVector v2) {
    double dotValue = 0.0;
    int[] v2Indices = v2.getIndices();
    for (int idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompIntFloatVector v1, IntFloatVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      float[] v2Values = v2.getStorage().getValues();
      for (IntFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getFloatValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getFloatValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getFloatValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntFloatVector v1, IntLongVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (IntFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getFloatValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getLongValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getFloatValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntFloatVector v1, IntIntVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (IntFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getFloatValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getFloatValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntLongVector v1, IntDummyVector v2) {
    double dotValue = 0.0;
    int[] v2Indices = v2.getIndices();
    for (int idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompIntLongVector v1, IntLongVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (IntLongVector part : v1.getPartitions()) {
        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getLongValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getLongValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntLongVector part : v1.getPartitions()) {
        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getLongValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntLongVector v1, IntIntVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (IntLongVector part : v1.getPartitions()) {
        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getLongValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntLongVector part : v1.getPartitions()) {
        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getLongValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompIntIntVector v1, IntDummyVector v2) {
    double dotValue = 0.0;
    int[] v2Indices = v2.getIndices();
    for (int idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompIntIntVector v1, IntIntVector v2) {
    double dotValue = 0.0;
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (IntIntVector part : v1.getPartitions()) {
        if (part.isDense()) {
          int[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2Values[idx];
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2IntMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getIntValue() * v2Values[idx];
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          int[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2Values[idx];
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int idx = entry.getIntKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      int[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      int base = 0;
      for (IntIntVector part : v1.getPartitions()) {
        if (part.isDense()) {
          int[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            int idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2IntMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = base + entry.getIntKey();
            dotValue += entry.getIntValue() * v2.get(idx);
          }
        } else { // isSorted
          int[] partIndices = part.getStorage().getIndices();
          int[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            int idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongDoubleVector v1, LongDummyVector v2) {
    double dotValue = 0.0;
    long[] v2Indices = v2.getIndices();
    for (long idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompLongDoubleVector v1, LongDoubleVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getDoubleValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongDoubleVector v1, LongFloatVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getFloatValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongDoubleVector v1, LongLongVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getLongValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongDoubleVector v1, LongIntVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongDoubleVector part : v1.getPartitions()) {
        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2DoubleMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getDoubleValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          double[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongFloatVector v1, LongDummyVector v2) {
    double dotValue = 0.0;
    long[] v2Indices = v2.getIndices();
    for (long idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompLongFloatVector v1, LongFloatVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getFloatValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getFloatValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongFloatVector v1, LongLongVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getLongValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getFloatValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongFloatVector v1, LongIntVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongFloatVector part : v1.getPartitions()) {
        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2FloatMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getFloatValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          float[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongLongVector v1, LongDummyVector v2) {
    double dotValue = 0.0;
    long[] v2Indices = v2.getIndices();
    for (long idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompLongLongVector v1, LongLongVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2LongMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getLongValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongLongVector part : v1.getPartitions()) {
        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getLongValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongLongVector v1, LongIntVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongLongVector part : v1.getPartitions()) {
        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2LongMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getLongValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          long[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

  private static double apply(CompLongIntVector v1, LongDummyVector v2) {
    double dotValue = 0.0;
    long[] v2Indices = v2.getIndices();
    for (long idx : v2Indices) {
      dotValue += v1.get(idx);
    }

    return dotValue;
  }

  private static double apply(CompLongIntVector v1, LongIntVector v2) {
    double dotValue = 0.0;
    if (v2.isSparse() && v1.size() > v2.size()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry entry = iter.next();
        long idx = entry.getLongKey();
        dotValue += v1.get(idx) * entry.getIntValue();
      }
    } else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted
      long[] v2Indices = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        dotValue += v1.get(idx) * v2Values[i];
      }
    } else {
      long base = 0;
      for (LongIntVector part : v1.getPartitions()) {
        if (part.isDense()) {
          int[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partValues.length; i++) {
            long idx = base + i;
            dotValue += partValues[i] * v2.get(idx);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2IntMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Long2IntMap.Entry entry = iter.next();
            long idx = base + entry.getLongKey();
            dotValue += entry.getIntValue() * v2.get(idx);
          }
        } else { // isSorted
          long[] partIndices = part.getStorage().getIndices();
          int[] partValues = part.getStorage().getValues();
          for (int i = 0; i < partIndices.length; i++) {
            long idx = base + partIndices[i];
            dotValue += partValues[i] * v2.get(idx);
          }
        }

        base += part.getDim();
      }
    }

    return dotValue;
  }

}