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


package com.tencent.angel.psagent;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import org.junit.Test;

/**
 * Created by payniexiao on 2018/7/23.
 */
public class MapTest {
  public static int len = 1000000;

  @Test public void testMap() {
    int[] indices = new int[len];
    double[] values = new double[len];
    for (int i = 0; i < len; i++) {
      indices[i] = i;
      values[i] = i;
    }

    long ts = System.currentTimeMillis();
    Int2DoubleOpenHashMap map = new Int2DoubleOpenHashMap(indices, values);
    System.out.println("put use time = " + (System.currentTimeMillis() - ts));
  }
}
