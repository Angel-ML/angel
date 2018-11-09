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
import com.tencent.angel.ml.math2.storage.IntLongSparseVectorStorage;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class ReduceOPTest {

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
      longValues[i] = rand.nextInt(100);
    }

    intValues = new int[capacity];
    for (int i = 0; i < intValues.length; i++) {
      intValues[i] = rand.nextInt(100);
    }
  }

  public void stats(int[] intValues) {
    double sum1 = 0.0;
    double sum2 = 0.0;
    double maxVal = Double.MIN_VALUE;
    double minVal = Double.MAX_VALUE;

    for (int v : intValues) {
      sum1 += v;
      sum2 += 1.0 * v * v;
      if (v > maxVal)
        maxVal = v;
      if (v < minVal)
        minVal = v;
    }

    double avg = sum1 / intValues.length;
    double norm = Math.sqrt(sum2);
    double std = Math.sqrt(sum2 / intValues.length - avg * avg);

    System.out.println(String
      .format("intValues::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, norm:%.3f", sum1,
        maxVal, minVal, std, avg, norm));
  }

  public void stats(long[] longValues) {
    double sum1 = 0.0;
    double sum2 = 0.0;
    double maxVal = Double.MIN_VALUE;
    double minVal = Double.MAX_VALUE;

    for (long v : longValues) {
      sum1 += v;
      sum2 += 1.0 * v * v;
      if (v > maxVal)
        maxVal = v;
      if (v < minVal)
        minVal = v;
    }

    double avg = sum1 / longValues.length;
    double norm = Math.sqrt(sum2);
    double std = Math.sqrt(sum2 / longValues.length - avg * avg);

    System.out.println(String
      .format("longValues::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, norm:%.3f", sum1,
        maxVal, minVal, std, avg, norm));
  }

  public void stats(float[] floatValues) {
    double sum1 = 0.0;
    double sum2 = 0.0;
    double maxVal = Double.MIN_VALUE;
    double minVal = Double.MAX_VALUE;

    for (float v : floatValues) {
      sum1 += v;
      sum2 += 1.0 * v * v;
      if (v > maxVal)
        maxVal = v;
      if (v < minVal)
        minVal = v;
    }

    double avg = sum1 / floatValues.length;
    double norm = Math.sqrt(sum2);
    double std = Math.sqrt(sum2 / floatValues.length - avg * avg);

    System.out.println(String
      .format("floatValues::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, norm:%.3f", sum1,
        maxVal, minVal, std, avg, norm));
  }

  public void stats(double[] doubleValues) {
    double sum1 = 0.0;
    double sum2 = 0.0;
    double maxVal = Double.MIN_VALUE;
    double minVal = Double.MAX_VALUE;

    for (double v : doubleValues) {
      sum1 += v;
      sum2 += 1.0 * v * v;
      if (v > maxVal)
        maxVal = v;
      if (v < minVal)
        minVal = v;
    }

    double avg = sum1 / doubleValues.length;
    double norm = Math.sqrt(sum2);
    double std = Math.sqrt(sum2 / doubleValues.length - avg * avg);

    System.out.println(String
      .format("doubleValues::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, norm:%.3f", sum1,
        maxVal, minVal, std, avg, norm));
  }

  @Test public void testall() {
    reduceIntDoubleVector();
    reduceLongDoubleVector();
    reduceIntFloatVector();
    reduceLongFloatVector();
    reduceIntLongVector();
    reduceLongLongVector();
    reduceIntIntVector();
    reduceLongIntVector();

    System.out.println("\n\n\n\n");
    stats(intValues);
    stats(longValues);
    stats(floatValues);
    stats(doubleValues);
  }

  private void reduceIntDoubleVector() {
    List<IntDoubleVector> list = new ArrayList<>();

    list.add(VFactory.denseDoubleVector(capacity));
    list.add(VFactory.denseDoubleVector(matrixId, rowId, clock, capacity));
    list.add(VFactory.denseDoubleVector(matrixId, rowId, clock, doubleValues));
    list.add(VFactory.denseDoubleVector(doubleValues));

    list.add(VFactory.sparseDoubleVector(dim));
    list.add(VFactory.sparseDoubleVector(dim, capacity));
    list.add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, capacity));
    list
      .add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, intrandIndices, doubleValues));
    list.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues));

    list.add(VFactory.sortedDoubleVector(dim));
    list.add(VFactory.sortedDoubleVector(dim, capacity));
    list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, capacity));
    list.add(
      VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, intsortedIndices, doubleValues));
    list.add(VFactory
      .sortedDoubleVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, doubleValues));
    list.add(VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues));
    list.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues));

    for (IntDoubleVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceLongDoubleVector() {
    List<LongDoubleVector> list = new ArrayList<>();

    list.add(VFactory.sparseLongKeyDoubleVector(dim));
    list.add(VFactory.sparseLongKeyDoubleVector(dim, capacity));
    list.add(VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory
      .sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, longrandIndices, doubleValues));
    list.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues));

    list.add(VFactory.sortedLongKeyDoubleVector(dim));
    list.add(VFactory.sortedLongKeyDoubleVector(dim, capacity));
    list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory
      .sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, longsortedIndices, doubleValues));
    list.add(VFactory
      .sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity, longsortedIndices,
        doubleValues));
    list.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues));
    list.add(VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues));

    for (LongDoubleVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceIntFloatVector() {
    List<IntFloatVector> list = new ArrayList<>();

    list.add(VFactory.denseFloatVector(capacity));
    list.add(VFactory.denseFloatVector(matrixId, rowId, clock, capacity));
    list.add(VFactory.denseFloatVector(matrixId, rowId, clock, floatValues));
    list.add(VFactory.denseFloatVector(floatValues));

    list.add(VFactory.sparseFloatVector(dim));
    list.add(VFactory.sparseFloatVector(dim, capacity));
    list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim, intrandIndices, floatValues));
    list.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues));

    list.add(VFactory.sortedFloatVector(dim));
    list.add(VFactory.sortedFloatVector(dim, capacity));
    list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, capacity));
    list
      .add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, intsortedIndices, floatValues));
    list.add(VFactory
      .sortedFloatVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, floatValues));
    list.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues));
    list.add(VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues));

    for (IntFloatVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceLongFloatVector() {
    List<LongFloatVector> list = new ArrayList<>();

    list.add(VFactory.sparseLongKeyFloatVector(dim));
    list.add(VFactory.sparseLongKeyFloatVector(dim, capacity));
    list.add(VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, capacity));
    list.add(
      VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, longrandIndices, floatValues));
    list.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues));

    list.add(VFactory.sortedLongKeyFloatVector(dim));
    list.add(VFactory.sortedLongKeyFloatVector(dim, capacity));
    list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory
      .sortedLongKeyFloatVector(matrixId, rowId, clock, dim, longsortedIndices, floatValues));
    list.add(VFactory
      .sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity, longsortedIndices,
        floatValues));
    list.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues));
    list.add(VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues));

    for (LongFloatVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%.3f, min:%.3f, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceIntLongVector() {
    List<IntLongVector> list = new ArrayList<>();

    list.add(VFactory.denseLongVector(capacity));
    list.add(VFactory.denseLongVector(matrixId, rowId, clock, capacity));
    list.add(VFactory.denseLongVector(matrixId, rowId, clock, longValues));
    list.add(VFactory.denseLongVector(longValues));

    list.add(VFactory.sparseLongVector(dim));
    list.add(VFactory.sparseLongVector(dim, capacity));
    list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim, intrandIndices, longValues));
    list.add(VFactory.sparseLongVector(dim, intrandIndices, longValues));

    list.add(VFactory.sortedLongVector(dim));
    list.add(VFactory.sortedLongVector(dim, capacity));
    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, intsortedIndices, longValues));
    list.add(VFactory
      .sortedLongVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, longValues));
    list.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues));
    list.add(VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues));

    for (IntLongVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%d, min:%d, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceLongLongVector() {
    List<LongLongVector> list = new ArrayList<>();

    list.add(VFactory.sparseLongKeyLongVector(dim));
    list.add(VFactory.sparseLongKeyLongVector(dim, capacity));
    list.add(VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, capacity));
    list.add(
      VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, longrandIndices, longValues));
    list.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues));

    list.add(VFactory.sortedLongKeyLongVector(dim));
    list.add(VFactory.sortedLongKeyLongVector(dim, capacity));
    list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity));
    list.add(
      VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, longsortedIndices, longValues));
    list.add(VFactory
      .sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity, longsortedIndices,
        longValues));
    list.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues));
    list.add(VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues));

    for (LongLongVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%d, min:%d, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceIntIntVector() {
    List<IntIntVector> list = new ArrayList<>();

    list.add(VFactory.denseIntVector(capacity));
    list.add(VFactory.denseIntVector(matrixId, rowId, clock, capacity));
    list.add(VFactory.denseIntVector(matrixId, rowId, clock, intValues));
    list.add(VFactory.denseIntVector(intValues));

    list.add(VFactory.sparseIntVector(dim));
    list.add(VFactory.sparseIntVector(dim, capacity));
    list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim, intrandIndices, intValues));
    list.add(VFactory.sparseIntVector(dim, intrandIndices, intValues));

    list.add(VFactory.sortedIntVector(dim));
    list.add(VFactory.sortedIntVector(dim, capacity));
    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity));
    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, intsortedIndices, intValues));
    list.add(
      VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, intValues));
    list.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues));
    list.add(VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues));

    for (IntIntVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%d, min:%d, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }

  private void reduceLongIntVector() {
    List<LongIntVector> list = new ArrayList<>();

    list.add(VFactory.sparseLongKeyIntVector(dim));
    list.add(VFactory.sparseLongKeyIntVector(dim, capacity));
    list.add(VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, capacity));
    list.add(
      VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, longrandIndices, intValues));
    list.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues));

    list.add(VFactory.sortedLongKeyIntVector(dim));
    list.add(VFactory.sortedLongKeyIntVector(dim, capacity));
    list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim));
    list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity));
    list.add(
      VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, longsortedIndices, intValues));
    list.add(VFactory
      .sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, intValues));
    list.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues));
    list.add(VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues));

    for (LongIntVector v : list) {
      System.out.println(String
        .format("%s::\t sum:%.3f, max:%d, min:%d, std:%.3f, avg:%.3f, numZeros:%d, norm:%.3f",
          v.getClass().getSimpleName(), v.sum(), v.max(), v.min(), v.std(), v.average(),
          v.numZeros(), v.norm()));
    }
  }
}
