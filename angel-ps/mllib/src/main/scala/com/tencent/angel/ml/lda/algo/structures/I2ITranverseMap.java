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

public class I2ITranverseMap extends IntTraverseHashMap {

  public int[] value;
  boolean[] used;
  public int[] idx;
  int[] poss;

  public I2ITranverseMap(int expected) {
    super(expected);
    value = new int[n];
    used = new boolean[n];
    idx = new int[n];
    poss = new int[n];
  }

  @Override public short get(short k) {
    return -1;
  }

  @Override public short get(int k) {
    int pos = (HashCommon.murmurHash3(k)) & mask;
    while (used[pos]) {
      if (key[pos] == k)
        return (short) value[pos];
      pos = (pos + 1) & mask;
    }
    return 0;
  }

  @Override public void put(short k, short v) {
    put((int) k, (int) v);
  }

  @Override public void put(int k, int v) {
    if (v == 0)
      return;

    int pos = (HashCommon.murmurHash3(k)) & mask;

    while (used[pos]) {
      if (key[pos] == k) {
        value[pos] = v;
        return;
      }
      pos = (pos + 1) & mask;
    }

    used[pos] = true;
    key[pos] = k;
    value[pos] = v;
    idx[size] = pos;
    poss[pos] = size;
    size++;
  }

  @Override public void rehash() {
    int[] kkey = key;
    int[] vvalue = value;
    key = new int[n];
    value = new int[n];

    Arrays.fill(used, false);
    int temp = size;
    size = 0;
    for (int i = 0; i < temp; i++) {
      int k = kkey[idx[i]];
      int v = vvalue[idx[i]];
      put(k, v);
    }
  }

  @Override public short dec(short k) {
    return dec((int) k);
  }

  @Override public short dec(int k) {
    int pos = (HashCommon.murmurHash3(k)) & mask;

    while (used[pos]) {
      if (key[pos] == k) {
        value[pos]--;
        if (value[pos] == 0) {
          size--;
          idx[poss[pos]] = idx[size];
          poss[idx[size]] = poss[pos];
        }
        return (short) value[pos];
      }

      pos = (pos + 1) & mask;
    }
    return 0;
  }

  @Override public short inc(int k) {
    int pos = (HashCommon.murmurHash3(k)) & mask;

    int cnt = 0;
    while (used[pos]) {
      if (key[pos] == k) {
        value[pos]++;
        if (value[pos] == 1) {
          idx[size] = pos;
          poss[pos] = size;
          size++;
        }

        return (short) value[pos];
      }

      cnt++;
      if (cnt > n) {
        rehash();
        return inc(k);
      }
      pos = (pos + 1) & mask;
    }

    key[pos] = k;
    value[pos] = 1;
    used[pos] = true;
    idx[size] = pos;
    poss[pos] = size;
    size++;
    return 1;
  }

  @Override public short inc(short k) {
    return inc((int) k);
  }

  @Override public int bytes() {
    int sum = 0;
    sum += key.length * 4;
    sum += value.length * 4;
    sum += used.length;
    sum += idx.length * 4;
    sum += poss.length * 4;
    return sum;
  }

  @Override public int getIntKey(int idx) {
    return key[this.idx[idx]];
  }

  @Override public int getVal(int idx) {
    return value[this.idx[idx]];
  }

  @Override public short getKey(int idx) {
    return (short) key[this.idx[idx]];
  }


}
