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
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class SimpleDotExecutor {
  public static double apply(Vector v1, Vector v2) {
    if (v1 instanceof IntDoubleVector && v2 instanceof IntDoubleVector) {
      return apply((IntDoubleVector) v1, (IntDoubleVector) v2);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntFloatVector) {
      return apply((IntDoubleVector) v1, (IntFloatVector) v2);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntLongVector) {
      return apply((IntDoubleVector) v1, (IntLongVector) v2);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntIntVector) {
      return apply((IntDoubleVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntDummyVector) {
      return apply((IntDoubleVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntFloatVector) {
      return apply((IntFloatVector) v1, (IntFloatVector) v2);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntLongVector) {
      return apply((IntFloatVector) v1, (IntLongVector) v2);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntIntVector) {
      return apply((IntFloatVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof IntFloatVector && v2 instanceof IntDummyVector) {
      return apply((IntFloatVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof IntLongVector && v2 instanceof IntLongVector) {
      return apply((IntLongVector) v1, (IntLongVector) v2);
    } else if (v1 instanceof IntLongVector && v2 instanceof IntIntVector) {
      return apply((IntLongVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof IntLongVector && v2 instanceof IntDummyVector) {
      return apply((IntLongVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof IntIntVector && v2 instanceof IntIntVector) {
      return apply((IntIntVector) v1, (IntIntVector) v2);
    } else if (v1 instanceof IntIntVector && v2 instanceof IntDummyVector) {
      return apply((IntIntVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof IntDummyVector && v2 instanceof IntDummyVector) {
      return apply((IntDummyVector) v1, (IntDummyVector) v2);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongDoubleVector) {
      return apply((LongDoubleVector) v1, (LongDoubleVector) v2);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongFloatVector) {
      return apply((LongDoubleVector) v1, (LongFloatVector) v2);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongLongVector) {
      return apply((LongDoubleVector) v1, (LongLongVector) v2);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongIntVector) {
      return apply((LongDoubleVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongDummyVector) {
      return apply((LongDoubleVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongFloatVector) {
      return apply((LongFloatVector) v1, (LongFloatVector) v2);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongLongVector) {
      return apply((LongFloatVector) v1, (LongLongVector) v2);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongIntVector) {
      return apply((LongFloatVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof LongFloatVector && v2 instanceof LongDummyVector) {
      return apply((LongFloatVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof LongLongVector && v2 instanceof LongLongVector) {
      return apply((LongLongVector) v1, (LongLongVector) v2);
    } else if (v1 instanceof LongLongVector && v2 instanceof LongIntVector) {
      return apply((LongLongVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof LongLongVector && v2 instanceof LongDummyVector) {
      return apply((LongLongVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof LongIntVector && v2 instanceof LongIntVector) {
      return apply((LongIntVector) v1, (LongIntVector) v2);
    } else if (v1 instanceof LongIntVector && v2 instanceof LongDummyVector) {
      return apply((LongIntVector) v1, (LongDummyVector) v2);
    } else if (v1 instanceof LongDummyVector && v2 instanceof LongDummyVector) {
      return apply((LongDummyVector) v1, (LongDummyVector) v2);
    } else {
      throw new AngelException("Vector type is not support!");
    }
  }

  private static double apply(IntDummyVector v1, IntDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    int[] keys1 = v1.getIndices();
    int[] keys2 = v2.getIndices();
    int v1Pointor = 0;
    int v2Pointor = 0;

    while (v1Pointor < keys1.length && v2Pointor < keys2.length) {
      if (keys1[v1Pointor] == keys2[v2Pointor]) {
        dot += 1.0;
        v2Pointor++;
        v1Pointor++;
      } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
        v2Pointor++;
      } else {
        v1Pointor++;
      }
    }
    return dot;
  }

  private static double apply(IntDoubleVector v1, IntDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    int[] idxs = v2.getIndices();
    for (int idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(IntDoubleVector v1, IntDoubleVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        dot += entry.getDoubleValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      double[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        dot += entry.getDoubleValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      double[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntDoubleVector v1, IntFloatVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        dot += entry.getFloatValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      float[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        dot += entry.getDoubleValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntDoubleVector v1, IntLongVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        dot += entry.getLongValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      long[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        dot += entry.getDoubleValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntDoubleVector v1, IntIntVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      double[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        dot += entry.getIntValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      double[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2DoubleMap.Entry entry = iter.next();
        dot += entry.getDoubleValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntFloatVector v1, IntDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    int[] idxs = v2.getIndices();
    for (int idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(IntFloatVector v1, IntFloatVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        dot += entry.getFloatValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      float[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        dot += entry.getFloatValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      float[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntFloatVector v1, IntLongVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        dot += entry.getLongValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      long[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        dot += entry.getFloatValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntFloatVector v1, IntIntVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      float[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        dot += entry.getIntValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      float[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry entry = iter.next();
        dot += entry.getFloatValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntLongVector v1, IntDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    int[] idxs = v2.getIndices();
    for (int idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(IntLongVector v1, IntLongVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        dot += entry.getLongValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      long[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        dot += entry.getLongValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntLongVector v1, IntIntVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      long[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        dot += entry.getIntValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      long[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2LongMap.Entry entry = iter.next();
        dot += entry.getLongValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(IntIntVector v1, IntDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    int[] idxs = v2.getIndices();
    for (int idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(IntIntVector v1, IntIntVector v2) {
    double dot = 0.0;
    if (v1.isDense() && v2.isDense()) {
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int length = v1Values.length;
      for (int i = 0; i < length; i++) {
        dot += v1Values[i] * v2Values[i];
      }
    } else if (v1.isDense() && v2.isSparse()) {
      int[] v1Values = v1.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        dot += entry.getIntValue() * v1Values[entry.getIntKey()];
      }
    } else if (v1.isDense() && v2.isSorted()) {
      int[] v1Values = v1.getStorage().getValues();
      int[] keys = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();
      int size = v2.size();
      for (int i = 0; i < size; i++) {
        dot += v2Values[i] * v1Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        dot += entry.getIntValue() * v2Values[entry.getIntKey()];
      }
    } else if (v1.isSorted() && v2.isDense()) {
      int[] keys = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      int[] v2Values = v2.getStorage().getValues();
      int size = v1.size();
      for (int i = 0; i < size; i++) {
        dot += v1Values[i] * v2Values[keys[i]];
      }
    } else if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v2.get(entry.getIntKey());
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Int2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v2.get(entry.getIntKey());
        }
      } else {
        int[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        int size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        int[] keys = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        int size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getIntKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      int[] keys1 = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      int[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      int size1 = v1.size();
      int size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongDummyVector v1, LongDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    long[] keys1 = v1.getIndices();
    long[] keys2 = v2.getIndices();
    int v1Pointor = 0;
    int v2Pointor = 0;

    while (v1Pointor < keys1.length && v2Pointor < keys2.length) {
      if (keys1[v1Pointor] == keys2[v2Pointor]) {
        dot += 1.0;
        v2Pointor++;
        v1Pointor++;
      } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
        v2Pointor++;
      } else {
        v1Pointor++;
      }
    }
    return dot;
  }

  private static double apply(LongDoubleVector v1, LongDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    long[] idxs = v2.getIndices();
    for (long idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(LongDoubleVector v1, LongDoubleVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      double[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongDoubleVector v1, LongFloatVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongDoubleVector v1, LongLongVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongDoubleVector v1, LongIntVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          dot += entry.getDoubleValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        double[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      double[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongFloatVector v1, LongDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    long[] idxs = v2.getIndices();
    for (long idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(LongFloatVector v1, LongFloatVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      float[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongFloatVector v1, LongLongVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongFloatVector v1, LongIntVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2FloatMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          dot += entry.getFloatValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        float[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      float[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongLongVector v1, LongDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    long[] idxs = v2.getIndices();
    for (long idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(LongLongVector v1, LongLongVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      long[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongLongVector v1, LongIntVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2LongMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          dot += entry.getLongValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        long[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      long[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

  private static double apply(LongIntVector v1, LongDummyVector v2) {
    assert v1.getDim() == v2.getDim();
    double dot = 0.0;
    long[] idxs = v2.getIndices();
    for (long idx : idxs) {
      dot += v1.get(idx);
    }
    return dot;
  }

  private static double apply(LongIntVector v1, LongIntVector v2) {
    double dot = 0.0;
    if (v1.isSparse() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v2.get(entry.getLongKey());
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSparse() && v2.isSorted()) {
      if (v1.size() < v2.size()) {
        ObjectIterator<Long2IntMap.Entry> iter = v1.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v2.get(entry.getLongKey());
        }
      } else {
        long[] keys = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        long size = v2.size();
        for (int i = 0; i < size; i++) {
          dot += v2Values[i] * v1.get(keys[i]);
        }
      }
    } else if (v1.isSorted() && v2.isSparse()) {
      if (v1.size() < v2.size()) {
        long[] keys = v1.getStorage().getIndices();
        int[] v1Values = v1.getStorage().getValues();
        long size = v1.size();
        for (int i = 0; i < size; i++) {
          dot += v1Values[i] * v2.get(keys[i]);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          dot += entry.getIntValue() * v1.get(entry.getLongKey());
        }
      }
    } else if (v1.isSorted() && v2.isSorted()) {
      long[] keys1 = v1.getStorage().getIndices();
      int[] v1Values = v1.getStorage().getValues();
      long[] keys2 = v2.getStorage().getIndices();
      int[] v2Values = v2.getStorage().getValues();

      int v1Pointor = 0;
      int v2Pointor = 0;
      long size1 = v1.size();
      long size2 = v2.size();

      while (v1Pointor < size1 && v2Pointor < size2) {
        if (keys1[v1Pointor] == keys2[v2Pointor]) {
          dot += v1Values[v1Pointor] * v2Values[v2Pointor];
          v2Pointor++;
          v1Pointor++;
        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {
          v2Pointor++;
        } else {
          v1Pointor++;
        }
      }
    } else {
      throw new AngelException("the operation is not support!");
    }

    return dot;
  }

}
