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


package com.tencent.angel.ml.lda.algo.structures;

import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;

public abstract class ShortTraverseHashMap extends TraverseHashMap {
  public short[] key;
  public short size;
  public short n;
  public int mask;

  public ShortTraverseHashMap(int expected) {
    super(expected);
    this.n = (short) HashCommon.arraySize(expected, 1.0F);
    this.size = 0;
    this.mask = n - 1;
    this.key = new short[n];
    Arrays.fill(key, (short) -1);
  }

  @Override public boolean used(int pos) {
    return key[pos] != -1;
  }

  @Override protected int idx(int pos) {
    return n + pos;
  }

  @Override protected int poss(int pos) {
    return n * 2 + pos;
  }

  @Override public int size() {
    return size;
  }
}
