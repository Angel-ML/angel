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

package com.tencent.angel.ml.lda.algo.structures;

import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;

public class S2STraverseMap extends TraverseHashMap {

  public short[] value;
  boolean[] used;
  public short[] idx;
  short[] poss;

  public S2STraverseMap(short expected) {
    super(expected);
    value = new short[n];
    used = new boolean[n];
    idx = new short[n];
    poss = new short[n];
  }

  public S2STraverseMap(int expected) {
    this((short) expected);
  }

  public short get(short k) {
    // The starting point
    int pos = (HashCommon.murmurHash3(k)) & mask;

    // There's always an unused entry.
    while (used[pos]) {
      if (key[pos] == k) {
        return value[pos];
      }
      pos = (pos + 1) & mask;
    }
    return 0;
  }

  public short get(int k) {
    return get((short) k);
  }

  public void put(final int k, final int v) {
    put((short) k, (short) v);
  }

  public void put(final short k, final short v) {
    if (v == 0)
      return;

    // The starting point
    int pos = (HashCommon.murmurHash3(k)) & mask;

    // There's always an unused entry.
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
    idx[size] = (short) pos;
    poss[(short) pos] = size;
    size++;
  }

  public void rehash() {

    short[] kkey = key;
    short[] vvalue = value;

    // print();

    key = new short[n];
    value = new short[n];

    Arrays.fill(used, false);

    int temp = size;
    size = 0;

    for (int i = 0; i < temp; i++) {
      short k = kkey[idx[i]];
      short v = vvalue[idx[i]];
      put(k, v);
    }

  }

  public short dec(final int k) {
    return dec((short) k);
  }

  public short dec(final short k) {
    int pos = (HashCommon.murmurHash3(k)) & mask;

    while (used[pos]) {
      if (key[pos] == k) {
        value[pos]--;
        if (value[pos] == 0) {
          size--;
          idx[poss[pos]] = idx[size];
          poss[idx[size]] = poss[pos];
        }
        return value[pos];
      }

      pos = (pos + 1) & mask;
    }
    return 0;
  }

  public short inc(final int k) {
    return inc((short) k);
  }

  public short inc(final short k) {
    int pos = (HashCommon.murmurHash3(k)) & mask;

    int cnt = 0;
    while (used[pos]) {
      if (key[pos] == k) {
        value[pos]++;
        if (value[pos] == 1) {
          idx[size] = (short) pos;
          poss[(short) pos] = size;
          size++;
        }

        return value[pos];
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
    idx[size] = (short) pos;
    poss[(short) pos] = size;
    size++;
    return 1;
  }

  @Override public int bytes() {
    int sum = 0;
    sum += key.length * 2;
    sum += value.length * 2;
    sum += used.length;
    sum += idx.length * 2;
    sum += poss.length * 2;
    return sum;
  }

  @Override public short getKey(int idx) {
    return key[this.idx[idx]];
  }

  @Override public int getVal(int idx) {
    return value[this.idx[idx]];
  }
}
