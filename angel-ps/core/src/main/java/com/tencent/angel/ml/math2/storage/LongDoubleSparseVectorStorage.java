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
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class LongDoubleSparseVectorStorage implements LongDoubleVectorStorage {

  private Long2DoubleOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private long dim;

  public LongDoubleSparseVectorStorage() {
    super();
  }

  public LongDoubleSparseVectorStorage(long dim, Long2DoubleOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public LongDoubleSparseVectorStorage(long dim, int capacity) {
    this(dim, new Long2DoubleOpenHashMap(capacity));
  }

  public LongDoubleSparseVectorStorage(long dim) {
    this(dim, (int) Math.min(64, Math.max(dim, 0)));
  }

  public LongDoubleSparseVectorStorage(long dim, long[] indices, double[] values) {
    this(dim, new Long2DoubleOpenHashMap(indices, values));
  }

  @Override
  public double get(long idx) {
    return map.get(idx);
  }

  @Override
  public void set(long idx, double value) {
    map.put(idx, value);
  }

  @Override
  public long[] getIndices() {
    return map.keySet().toLongArray();
  }

  @Override
  public ObjectIterator<Long2DoubleMap.Entry> entryIterator() {
    return map.long2DoubleEntrySet().fastIterator();
  }

  @Override
  public LongDoubleVectorStorage clone() {
    return new LongDoubleSparseVectorStorage(dim, map.clone());
  }

  @Override
  public LongDoubleVectorStorage copy() {
    return new LongDoubleSparseVectorStorage(dim, map.clone());
  }


  @Override
  public LongDoubleVectorStorage oneLikeSparse() {
    int capacity = map.size();
    double[] oneLikeValues = new double[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongDoubleSparseVectorStorage(dim, map.keySet().toLongArray(), oneLikeValues);
  }

  @Override
  public LongDoubleVectorStorage oneLikeSorted() {
    int capacity = map.size();
    double[] oneLikeValues = new double[capacity];
    long[] indices = map.keySet().toLongArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override
  public LongDoubleVectorStorage oneLikeSparse(long dim, int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new LongDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongDoubleVectorStorage oneLikeSorted(long dim, int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new LongDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongDoubleVectorStorage oneLikeSparse(int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new LongDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongDoubleVectorStorage oneLikeSorted(int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new LongDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override
  public LongDoubleVectorStorage emptySparse() {
    return new LongDoubleSparseVectorStorage(dim, map.size());
  }

  @Override
  public LongDoubleVectorStorage emptySorted() {
    return new LongDoubleSortedVectorStorage(dim, map.size());
  }


  @Override
  public LongDoubleVectorStorage emptySparse(long dim, int capacity) {
    return new LongDoubleSparseVectorStorage(dim, capacity);
  }

  @Override
  public LongDoubleVectorStorage emptySorted(long dim, int capacity) {
    return new LongDoubleSortedVectorStorage(dim, capacity);
  }

  @Override
  public LongDoubleVectorStorage emptySparse(int capacity) {
    return new LongDoubleSparseVectorStorage(dim, capacity);
  }

  @Override
  public LongDoubleVectorStorage emptySorted(int capacity) {
    return new LongDoubleSortedVectorStorage(dim, capacity);
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
    return RowType.T_DOUBLE_SPARSE_LONGKEY;
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
  public DoubleIterator valueIterator() {
    return map.values().iterator();
  }

  @Override
  public double[] getValues() {
    return map.values().toDoubleArray();
  }
}
