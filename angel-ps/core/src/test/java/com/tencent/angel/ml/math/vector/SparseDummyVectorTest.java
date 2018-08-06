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

package com.tencent.angel.ml.math.vector;

import org.junit.Test;

import java.util.Arrays;

import static com.tencent.angel.ml.Utils.genSortedIndexs;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class SparseDummyVectorTest {

  @Test
  public void set() throws Exception {
    SparseDummyVector vec = new SparseDummyVector(10000);

    int[] indexs = genSortedIndexs(300, 10000);

    for (int idx: indexs)
      vec.set(idx, 1.0);

    int[] indexs_1 = vec.getIndices();

    for (int idx: indexs)
      assertTrue(Arrays.binarySearch(indexs_1, 0, indexs.length, idx) > -1);
  }


  @Test
  public void sparsity() throws Exception {
    SparseDummyVector vec = new SparseDummyVector(10000, 100);

    int[] indexs = genSortedIndexs(300, 10000);

    for (int idx: indexs)
      vec.set(idx, 1.0);

    assertEquals(0.03, vec.sparsity());
  }

}