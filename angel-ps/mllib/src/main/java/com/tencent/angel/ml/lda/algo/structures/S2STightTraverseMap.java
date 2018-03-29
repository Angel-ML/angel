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


public class S2STightTraverseMap extends TraverseHashMap {


  short[] values;

  public S2STightTraverseMap(short expected) {
    super(expected);
    values = new short[n * 3];
  }

  public S2STightTraverseMap(int expected) {
    this((short) expected);
  }

  public short get(final int k) {
    return get((short) k);
  }

  public short get(final short k) {
    // The starting point
    int pos = (HashCommon.murmurHash3(k)) & mask;

    // There's always an unused entry.
    int cnt = 0;
    while (used(pos)) {
      if (key[pos] == k) {
        return values[pos];
      }
      pos = (pos + 1) & mask;
      cnt++;

      if (cnt > n) {
        rehash();
        return get(k);
      }
    }
    return 0;
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
    while (used(pos)) {
      if (key[pos] == k) {
        values[pos] = v;
        return;
      }
      pos = (pos + 1) & mask;
    }

    key[pos] = k;
    values[pos] = v;
    values[size + n] = (short) pos;
    values[(short) pos + n * 2] = size;
    size++;
  }

  public void rehash() {

    short[] kkey = key;
    short[] vvalue = values;


    key = new short[n];
    values = new short[n * 3];

    Arrays.fill(key, (short) -1);

    int temp = size;
    size = 0;

    for (int i = 0; i < temp; i++) {
      short k = kkey[vvalue[idx(i)]];
      short v = vvalue[vvalue[idx(i)]];
      put(k, v);
    }

  }

  public short dec(final int k) {
    return dec((short) k);
  }

  public short dec(final short k) {
    int pos = (HashCommon.murmurHash3(k)) & mask;

    while (used(pos)) {
      if (key[pos] == k) {
        values[pos]--;
        if (values[pos] == 0) {
          size--;
          values[idx(values[poss(pos)])] = values[idx(size)];
          values[poss(values[idx(size)])] = values[poss(pos)];
        }
        return values[pos];
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
    while (used(pos)) {
      if (key[pos] == k) {
        values[pos]++;
        if (values[pos] == 1) {
          values[idx(size)] = (short) pos;
          values[poss(pos)] = size;
          size++;
        }

        return values[pos];
      }

      cnt++;
      if (cnt > n) {
        rehash();
        return inc(k);
      }
      pos = (pos + 1) & mask;
    }

    key[pos] = k;
    values[pos] = 1;
    values[idx(size)] = (short) pos;
    values[poss(pos)] = size;
    size++;
    return 1;
  }

  @Override public int bytes() {
    int sum = 0;
    sum += key.length * 2;
    sum += values.length * 2;
    return sum;
  }

  @Override public short getKey(int idx) {
    return key[values[idx(idx)]];
  }

  @Override public int getVal(int idx) {
    return values[values[idx(idx)]];
  }

}
