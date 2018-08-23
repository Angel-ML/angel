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
import com.tencent.angel.ml.math2.ufuncs.TransFuncs;
import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class UnaryOPTest {
  private static int capacity;
  private static int dim;

  private static int[] intrandIndices;
  private static long[] longrandIndices;
  private static int[] intsortedIndices;
  private static long[] longsortedIndices;

  private static double[] doubleValues;
  private static float[] floatValues;

  @BeforeClass public static void init() {
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
      doubleValues[i] = Math.abs(rand.nextDouble()) + 0.01;
    }

    floatValues = new float[capacity];
    for (int i = 0; i < floatValues.length; i++) {
      floatValues[i] = Math.abs(rand.nextFloat()) + 0.01f;
    }
  }

  @Test public void testall() {
    unaryIntDoubleVector();
    unaryLongFloatVector();
  }

  private void unaryIntDoubleVector() {
    List<IntDoubleVector> list = new ArrayList<>();

    list.add(VFactory.denseDoubleVector(doubleValues));
    list.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues));
    list.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues));

    for (IntDoubleVector v : list) {
      Ufuncs.exp(v);
      Ufuncs.iexp(v);
      Ufuncs.log(v);
      Ufuncs.ilog(v);
      Ufuncs.log1p(v);
      Ufuncs.ilog1p(v);
      TransFuncs.sigmoid(v);
      TransFuncs.isigmoid(v);
      Ufuncs.pow(v, 2.0);
      Ufuncs.ipow(v, 1.5);
      Ufuncs.softthreshold(v, 1.5);
      Ufuncs.isoftthreshold(v, 1.5);
      Ufuncs.sqrt(v);
      Ufuncs.isqrt(v);
      Ufuncs.sadd(v, 0.5);
      Ufuncs.isadd(v, 0.5);
      Ufuncs.ssub(v, 0.5);
      Ufuncs.issub(v, 0.5);
      Ufuncs.smul(v, 0.5);
      Ufuncs.ismul(v, 0.5);
      Ufuncs.sdiv(v, 0.5);
      Ufuncs.isdiv(v, 0.5);
    }
  }

  private void unaryLongFloatVector() {
    List<LongFloatVector> list = new ArrayList<>();

    list.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues));
    list.add(VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues));

    for (LongFloatVector v : list) {
      if (v.isSparse() || v.isSorted()) {
        Ufuncs.pow(v, 2.0);
        Ufuncs.ipow(v, 1.5);
        Ufuncs.sqrt(v);
        Ufuncs.isqrt(v);
        Ufuncs.smul(v, 0.5);
        Ufuncs.ismul(v, 0.5);
        Ufuncs.sdiv(v, 0.5);
        Ufuncs.isdiv(v, 0.5);
      } else {
        Ufuncs.exp(v);
        Ufuncs.iexp(v);
        Ufuncs.log(v);
        Ufuncs.ilog(v);
        Ufuncs.log1p(v);
        Ufuncs.ilog1p(v);
        TransFuncs.sigmoid(v);
        TransFuncs.isigmoid(v);
        Ufuncs.pow(v, 2.0);
        Ufuncs.ipow(v, 1.5);
        Ufuncs.softthreshold(v, 1.5);
        Ufuncs.isoftthreshold(v, 1.5);
        Ufuncs.sqrt(v);
        Ufuncs.isqrt(v);
        Ufuncs.sadd(v, 0.5);
        Ufuncs.isadd(v, 0.5);
        Ufuncs.ssub(v, 0.5);
        Ufuncs.issub(v, 0.5);
        Ufuncs.smul(v, 0.5);
        Ufuncs.ismul(v, 0.5);
        Ufuncs.sdiv(v, 0.5);
        Ufuncs.isdiv(v, 0.5);
      }
    }
  }
}
