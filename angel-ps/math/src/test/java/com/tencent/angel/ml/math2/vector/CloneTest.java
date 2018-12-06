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

import org.junit.Test;

public class CloneTest {

  @Test public void cloneTest() {
    int n = 10000000;
    double[] clres;
    double[] cpres = new double[n];
    for (int i = 0; i < n; i++) {
      cpres[i] = Math.random();
    }

    long start, stop, t1 = 0L, t2 = 0L;
    for (int i = 0; i < 10; i++) {
      start = System.currentTimeMillis();
      clres = cpres.clone();
      stop = System.currentTimeMillis();
      t1 += stop - start;

      start = System.currentTimeMillis();
      System.arraycopy(clres, 0, cpres, 0, cpres.length);
      stop = System.currentTimeMillis();
      t2 += stop - start;
    }

    System.out.println(t1);
    System.out.println(t2);
  }
}
