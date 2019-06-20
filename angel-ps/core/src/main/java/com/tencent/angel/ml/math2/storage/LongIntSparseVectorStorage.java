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


package com.tencent.angel.ml.math2.storage;

import com.tencent.angel.ml.matrix.RowType;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class LongIntSparseVectorStorage implements LongIntVectorStorage {

  private Long2IntOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private long dim;

  public LongIntSparseVectorStorage() {
    super();
  }

  public LongIntSparseVectorStorage(long dim, Long2IntOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public LongIntSparseVectorStorage(long dim, int capacity) {
    this(dim, new Long2IntOpenHashMap(capacity));
  }

  public LongIntSparseVectorStorage(long dim) {
    this(dim, (int) Math.min(64, Math.max(dim, 0)));
  }

  public LongIntSparseVectorStorage(long dim, long[] indices, int[] values) {
    this(dim, new Long2IntOpenHashMap(indices, values));
  }

  @Override
  public int get(long idx) {
    return map.get(idx);
  }

  @Override
  public void set(long idx, int value) {
    map.put(idx, value);
  }

  @Override
  public long[] getIndices() {
    return map.keySet().toLongArray();
  }

  @Override
  public ObjectIterator<Long2IntMap.Entry> entryIterator() {
    return map.long2IntEntrySet().fastIterator();
  }

  @Override
  public LongIntVectorStorage clone() {
    return new LongIntSparseVectorStorage(dim, map.clone());
  }

  @Override
  public LongIntVectorStorage copy() {
    return new LongIntSparseVectorStorage(dim, map.clone());
  }


  @Override
  public LongIntVectorStorage oneLikeSparse() {
    int capacity = map.size();
    int[] oneLikeValues = new int[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongIntSparseVectorStorage(dim, map.keySet().toLongArray(), oneLikeValues);
  }

  @Override
  public LongIntVectorStorage oneLikeSorted() {
    int capacity = map.size();
    int[] oneLikeValues = new int[capacity];
    long[] indices = map.keySet().toLongArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongIntSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override
  public LongIntVectorStorage oneLikeSparse(long dim, int capacity) {
    int[] oneLikeValues = new int[capacity];
    long[] indices = new long[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      long idx = rand.nextInt((int) dim);
      if (!set.contains(idx)) {
        indices[j] = idx;
        set.add(idx);
        j++;
      }
    }
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongIntVectorStorage oneLikeSorted(long dim, int capacity) {
    int[] oneLikeValues = new int[capacity];
    long[] indices = new long[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      long idx = rand.nextInt((int) dim);
      if (!set.contains(idx)) {
        indices[j] = idx;
        set.add(idx);
        j++;
      }
    }
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongIntVectorStorage oneLikeSparse(int capacity) {
    int[] oneLikeValues = new int[capacity];
    long[] indices = new long[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      int idx = rand.nextInt((int) dim);
      if (!set.contains(idx)) {
        indices[j] = idx;
        set.add(idx);
        j++;
      }
    }
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongIntVectorStorage oneLikeSorted(int capacity) {
    int[] oneLikeValues = new int[capacity];
    long[] indices = new long[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      int idx = rand.nextInt((int) dim);
      if (!set.contains(idx)) {
        indices[j] = idx;
        set.add(idx);
        j++;
      }
    }
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongIntSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override
  public LongIntVectorStorage emptySparse() {
    return new LongIntSparseVectorStorage(dim, map.size());
  }

  @Override
  public LongIntVectorStorage emptySorted() {
    return new LongIntSortedVectorStorage(dim, map.size());
  }


  @Override
  public LongIntVectorStorage emptySparse(long dim, int capacity) {
    return new LongIntSparseVectorStorage(dim, capacity);
  }

  @Override
  public LongIntVectorStorage emptySorted(long dim, int capacity) {
    return new LongIntSortedVectorStorage(dim, capacity);
  }

  @Override
  public LongIntVectorStorage emptySparse(int capacity) {
    return new LongIntSparseVectorStorage(dim, capacity);
  }

  @Override
  public LongIntVectorStorage emptySorted(int capacity) {
    return new LongIntSortedVectorStorage(dim, capacity);
  }

  @Override
  public LongSet indexIterator() {
    return map.keySet();
  }

  @Override
  public int size() {
    return map.size();
  }

  public void setSize(int size) {
  }

  @Override
  public boolean hasKey(long key) {
    return map.containsKey(key);
  }

  @Override
  public RowType getType() {
    return RowType.T_INT_SPARSE_LONGKEY;
  }

  @Override
  public boolean isDense() {
    return flag == 1;
  }

  @Override
  public boolean isSparse() {
    return flag == 2;
  }

  @Override
  public boolean isSorted() {
    return flag == 4;
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public IntIterator valueIterator() {
    return map.values().iterator();
  }

  @Override
  public int[] getValues() {
    return map.values().toIntArray();
  }
}
