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


package com.tencent.angel.ml.math2.vector;

import com.tencent.angel.ml.math2.VFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class CreateVectorTest {
  private static int matrixId;
  private static int rowId;
  private static int clock;
  private static int capacity;
  private static int dim;

  private static int[] intrandIndices;
  private static long[] longrandIndices;
  private static int[] intsortedIndices;
  private static long[] longsortedIndices;

  private static int[] intValues;
  private static long[] longValues;
  private static float[] floatValues;
  private static double[] doubleValues;

  @BeforeClass public static void init() {
    matrixId = 0;
    rowId = 0;
    clock = 0;
    capacity = 1000;
    dim = capacity * 1000;

    Random rand = new Random();
    intrandIndices = new int[capacity];
    HashSet<Integer> set = new HashSet<>();
    int idx = 0;
    while (set.size() < capacity) {
      int t = rand.nextInt(dim);
      if (!set.contains(t)) {
        intrandIndices[idx] = t;
        set.add(t);
        idx++;
      }
    }

    longrandIndices = new long[capacity];
    set.clear();
    idx = 0;
    while (set.size() < capacity) {
      int t = rand.nextInt(dim);
      if (!set.contains(t)) {
        longrandIndices[idx] = t;
        set.add(t);
        idx++;
      }
    }

    intsortedIndices = new int[capacity];
    System.arraycopy(intrandIndices, 0, intsortedIndices, 0, capacity);
    Arrays.sort(intsortedIndices);

    longsortedIndices = new long[capacity];
    System.arraycopy(longrandIndices, 0, longsortedIndices, 0, capacity);
    Arrays.sort(longsortedIndices);

    doubleValues = new double[capacity];
    for (int i = 0; i < doubleValues.length; i++) {
      doubleValues[i] = rand.nextDouble();
    }

    floatValues = new float[capacity];
    for (int i = 0; i < floatValues.length; i++) {
      floatValues[i] = rand.nextFloat();
    }

    longValues = new long[capacity];
    for (int i = 0; i < longValues.length; i++) {
      longValues[i] = rand.nextInt();
    }

    intValues = new int[capacity];
    for (int i = 0; i < intValues.length; i++) {
      intValues[i] = rand.nextInt();
    }
  }

  @Test public void testall() {
    createIntDoubleVector();
    createLongDoubleVector();
    createIntFloatVector();
    createLongFloatVector();
    createIntLongVector();
    createLongLongVector();
    createIntIntVector();
    createLongIntVector();
  }

  private void createIntDoubleVector() {
    IntDoubleVector v1 = VFactory.denseDoubleVector(capacity);
    IntDoubleVector v2 = VFactory.denseDoubleVector(matrixId, rowId, clock, capacity);
    IntDoubleVector v3 = VFactory.denseDoubleVector(matrixId, rowId, clock, doubleValues);
    IntDoubleVector v4 = VFactory.denseDoubleVector(doubleValues);

    IntDoubleVector v5 = VFactory.sparseDoubleVector(dim);
    IntDoubleVector v6 = VFactory.sparseDoubleVector(dim, capacity);
    IntDoubleVector v7 = VFactory.sparseDoubleVector(matrixId, rowId, clock, dim);
    IntDoubleVector v8 = VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, capacity);
    IntDoubleVector v9 =
      VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, intrandIndices, doubleValues);
    IntDoubleVector v10 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues);

    IntDoubleVector v11 = VFactory.sortedDoubleVector(dim);
    IntDoubleVector v12 = VFactory.sortedDoubleVector(dim, capacity);
    IntDoubleVector v13 = VFactory.sortedDoubleVector(matrixId, rowId, clock, dim);
    IntDoubleVector v14 = VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, capacity);
    IntDoubleVector v15 =
      VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, intsortedIndices, doubleValues);
    IntDoubleVector v16 = VFactory
      .sortedDoubleVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, doubleValues);
    IntDoubleVector v17 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues);
    IntDoubleVector v18 =
      VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues);
  }

  private void createLongDoubleVector() {
    LongDoubleVector v5 = VFactory.sparseLongKeyDoubleVector(dim);
    LongDoubleVector v6 = VFactory.sparseLongKeyDoubleVector(dim, capacity);
    LongDoubleVector v7 = VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim);
    LongDoubleVector v8 = VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity);
    LongDoubleVector v9 = VFactory
      .sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, longrandIndices, doubleValues);
    LongDoubleVector v10 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues);

    LongDoubleVector v11 = VFactory.sortedLongKeyDoubleVector(dim);
    LongDoubleVector v12 = VFactory.sortedLongKeyDoubleVector(dim, capacity);
    LongDoubleVector v13 = VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim);
    LongDoubleVector v14 =
      VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity);
    LongDoubleVector v15 = VFactory
      .sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, longsortedIndices, doubleValues);
    LongDoubleVector v16 = VFactory
      .sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity, longsortedIndices,
        doubleValues);
    LongDoubleVector v17 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues);
    LongDoubleVector v18 =
      VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues);
  }

  private void createIntFloatVector() {
    IntFloatVector v1 = VFactory.denseFloatVector(capacity);
    IntFloatVector v2 = VFactory.denseFloatVector(matrixId, rowId, clock, capacity);
    IntFloatVector v3 = VFactory.denseFloatVector(matrixId, rowId, clock, floatValues);
    IntFloatVector v4 = VFactory.denseFloatVector(floatValues);

    IntFloatVector v5 = VFactory.sparseFloatVector(dim);
    IntFloatVector v6 = VFactory.sparseFloatVector(dim, capacity);
    IntFloatVector v7 = VFactory.sparseFloatVector(matrixId, rowId, clock, dim);
    IntFloatVector v8 = VFactory.sparseFloatVector(matrixId, rowId, clock, dim, capacity);
    IntFloatVector v9 =
      VFactory.sparseFloatVector(matrixId, rowId, clock, dim, intrandIndices, floatValues);
    IntFloatVector v10 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues);

    IntFloatVector v11 = VFactory.sortedFloatVector(dim);
    IntFloatVector v12 = VFactory.sortedFloatVector(dim, capacity);
    IntFloatVector v13 = VFactory.sortedFloatVector(matrixId, rowId, clock, dim);
    IntFloatVector v14 = VFactory.sortedFloatVector(matrixId, rowId, clock, dim, capacity);
    IntFloatVector v15 =
      VFactory.sortedFloatVector(matrixId, rowId, clock, dim, intsortedIndices, floatValues);
    IntFloatVector v16 = VFactory
      .sortedFloatVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, floatValues);
    IntFloatVector v17 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues);
    IntFloatVector v18 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues);
  }

  private void createLongFloatVector() {
    LongFloatVector v5 = VFactory.sparseLongKeyFloatVector(dim);
    LongFloatVector v6 = VFactory.sparseLongKeyFloatVector(dim, capacity);
    LongFloatVector v7 = VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim);
    LongFloatVector v8 = VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, capacity);
    LongFloatVector v9 =
      VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, longrandIndices, floatValues);
    LongFloatVector v10 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues);

    LongFloatVector v11 = VFactory.sortedLongKeyFloatVector(dim);
    LongFloatVector v12 = VFactory.sortedLongKeyFloatVector(dim, capacity);
    LongFloatVector v13 = VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim);
    LongFloatVector v14 = VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity);
    LongFloatVector v15 = VFactory
      .sortedLongKeyFloatVector(matrixId, rowId, clock, dim, longsortedIndices, floatValues);
    LongFloatVector v16 = VFactory
      .sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity, longsortedIndices,
        floatValues);
    LongFloatVector v17 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues);
    LongFloatVector v18 =
      VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues);
  }

  private void createIntLongVector() {
    IntLongVector v1 = VFactory.denseLongVector(capacity);
    IntLongVector v2 = VFactory.denseLongVector(matrixId, rowId, clock, capacity);
    IntLongVector v3 = VFactory.denseLongVector(matrixId, rowId, clock, longValues);
    IntLongVector v4 = VFactory.denseLongVector(longValues);

    IntLongVector v5 = VFactory.sparseLongVector(dim);
    IntLongVector v6 = VFactory.sparseLongVector(dim, capacity);
    IntLongVector v7 = VFactory.sparseLongVector(matrixId, rowId, clock, dim);
    IntLongVector v8 = VFactory.sparseLongVector(matrixId, rowId, clock, dim, capacity);
    IntLongVector v9 =
      VFactory.sparseLongVector(matrixId, rowId, clock, dim, intrandIndices, longValues);
    IntLongVector v10 = VFactory.sparseLongVector(dim, intrandIndices, longValues);

    IntLongVector v11 = VFactory.sortedLongVector(dim);
    IntLongVector v12 = VFactory.sortedLongVector(dim, capacity);
    IntLongVector v13 = VFactory.sortedLongVector(matrixId, rowId, clock, dim);
    IntLongVector v14 = VFactory.sortedLongVector(matrixId, rowId, clock, dim, capacity);
    IntLongVector v15 =
      VFactory.sortedLongVector(matrixId, rowId, clock, dim, intsortedIndices, longValues);
    IntLongVector v16 = VFactory
      .sortedLongVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, longValues);
    IntLongVector v17 = VFactory.sortedLongVector(dim, intsortedIndices, longValues);
    IntLongVector v18 = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues);
  }

  private void createLongLongVector() {
    LongLongVector v5 = VFactory.sparseLongKeyLongVector(dim);
    LongLongVector v6 = VFactory.sparseLongKeyLongVector(dim, capacity);
    LongLongVector v7 = VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim);
    LongLongVector v8 = VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, capacity);
    LongLongVector v9 =
      VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, longrandIndices, longValues);
    LongLongVector v10 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues);

    LongLongVector v11 = VFactory.sortedLongKeyLongVector(dim);
    LongLongVector v12 = VFactory.sortedLongKeyLongVector(dim, capacity);
    LongLongVector v13 = VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim);
    LongLongVector v14 = VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity);
    LongLongVector v15 =
      VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, longsortedIndices, longValues);
    LongLongVector v16 = VFactory
      .sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity, longsortedIndices,
        longValues);
    LongLongVector v17 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues);
    LongLongVector v18 =
      VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues);
  }

  private void createIntIntVector() {
    IntIntVector v1 = VFactory.denseIntVector(capacity);
    IntIntVector v2 = VFactory.denseIntVector(matrixId, rowId, clock, capacity);
    IntIntVector v3 = VFactory.denseIntVector(matrixId, rowId, clock, intValues);
    IntIntVector v4 = VFactory.denseIntVector(intValues);

    IntIntVector v5 = VFactory.sparseIntVector(dim);
    IntIntVector v6 = VFactory.sparseIntVector(dim, capacity);
    IntIntVector v7 = VFactory.sparseIntVector(matrixId, rowId, clock, dim);
    IntIntVector v8 = VFactory.sparseIntVector(matrixId, rowId, clock, dim, capacity);
    IntIntVector v9 =
      VFactory.sparseIntVector(matrixId, rowId, clock, dim, intrandIndices, intValues);
    IntIntVector v10 = VFactory.sparseIntVector(dim, intrandIndices, intValues);

    IntIntVector v11 = VFactory.sortedIntVector(dim);
    IntIntVector v12 = VFactory.sortedIntVector(dim, capacity);
    IntIntVector v13 = VFactory.sortedIntVector(matrixId, rowId, clock, dim);
    IntIntVector v14 = VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity);
    IntIntVector v15 =
      VFactory.sortedIntVector(matrixId, rowId, clock, dim, intsortedIndices, intValues);
    IntIntVector v16 =
      VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, intValues);
    IntIntVector v17 = VFactory.sortedIntVector(dim, intsortedIndices, intValues);
    IntIntVector v18 = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues);
  }

  private void createLongIntVector() {
    LongIntVector v5 = VFactory.sparseLongKeyIntVector(dim);
    LongIntVector v6 = VFactory.sparseLongKeyIntVector(dim, capacity);
    LongIntVector v7 = VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim);
    LongIntVector v8 = VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, capacity);
    LongIntVector v9 =
      VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, longrandIndices, intValues);
    LongIntVector v10 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues);

    LongIntVector v11 = VFactory.sortedLongKeyIntVector(dim);
    LongIntVector v12 = VFactory.sortedLongKeyIntVector(dim, capacity);
    LongIntVector v13 = VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim);
    LongIntVector v14 = VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity);
    LongIntVector v15 =
      VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, longsortedIndices, intValues);
    LongIntVector v16 = VFactory
      .sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, intValues);
    LongIntVector v17 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues);
    LongIntVector v18 =
      VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues);
  }
}
