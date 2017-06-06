/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.lda.structures;

import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;

public abstract class TraverseHashMap {

  public short size;
  public short[] key;
  public short n;
  public int mask;

  public TraverseHashMap(int expected) {
    this.n = (short) HashCommon.arraySize(expected, 1.0F);
    this.key = new short[n];
    this.size = 0;
    this.mask = n - 1;
    Arrays.fill(key, (short) -1);
  }

  public abstract short get(final short k);

  public abstract short get(final int k);

  public abstract void put(final short k, final short v);

  public abstract void rehash();

  public abstract short dec(final short k);

  public abstract short dec(final int k);

  public abstract short inc(final short k);

  public abstract short inc(final int k);

  public abstract int bytes();

  public abstract short getKey(int idx);

  public abstract short getVal(int idx);

  protected int idx(int pos) {
    return n + pos;
  }

  protected int poss(int pos) {
    return n * 2 + pos;
  }

  protected boolean used(int pos) {
    return key[pos] != -1;
  }

}
