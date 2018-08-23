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
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.floats.*;
import it.unimi.dsi.fastutil.doubles.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;
import java.util.HashSet;

public class IntLongSparseVectorStorage implements IntLongVectorStorage {
  private Int2LongOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntLongSparseVectorStorage(int dim, Int2LongOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public IntLongSparseVectorStorage(int dim, int capacity) {
    this(dim, new Int2LongOpenHashMap(capacity));
  }

  public IntLongSparseVectorStorage(int dim) {
    this(dim, Math.max(128, (int) (dim / 1000)));
  }

  public IntLongSparseVectorStorage(int dim, int[] indices, long[] values) {
    this(dim, new Int2LongOpenHashMap(indices, values));
  }

  @Override public long get(int idx) {
    return map.get(idx);
  }

  @Override public void set(int idx, long value) {
    map.put(idx, value);
  }

  @Override public int[] getIndices() {
    return map.keySet().toIntArray();
  }

  @Override public ObjectIterator<Int2LongMap.Entry> entryIterator() {
    return map.int2LongEntrySet().fastIterator();
  }

  @Override public IntLongVectorStorage clone() {
    return new IntLongSparseVectorStorage(dim, map.clone());
  }

  @Override public IntLongVectorStorage copy() {
    return new IntLongSparseVectorStorage(dim, map.clone());
  }

  @Override public IntLongVectorStorage oneLikeDense() {
    long[] oneLikeValues = new long[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntLongDenseVectorStorage(oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSparse() {
    int capacity = map.size();
    long[] oneLikeValues = new long[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntLongSparseVectorStorage(dim, map.keySet().toIntArray(), oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSorted() {
    int capacity = map.size();
    long[] oneLikeValues = new long[capacity];
    int[] indices = map.keySet().toIntArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeDense(int dim) {
    long[] oneLikeValues = new long[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntLongDenseVectorStorage(oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSparse(int dim, int capacity) {
    long[] oneLikeValues = new long[capacity];
    int[] indices = new int[capacity];
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
    return new IntLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSorted(int dim, int capacity) {
    long[] oneLikeValues = new long[capacity];
    int[] indices = new int[capacity];
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
    return new IntLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSparse(int capacity) {
    long[] oneLikeValues = new long[capacity];
    int[] indices = new int[capacity];
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
    return new IntLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSorted(int capacity) {
    long[] oneLikeValues = new long[capacity];
    int[] indices = new int[capacity];
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
    return new IntLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public IntLongVectorStorage emptyDense() {
    return new IntLongDenseVectorStorage(dim);
  }

  @Override public IntLongVectorStorage emptySparse() {
    return new IntLongSparseVectorStorage(dim, map.size());
  }

  @Override public IntLongVectorStorage emptySorted() {
    return new IntLongSortedVectorStorage(dim, map.size());
  }

  @Override public IntLongVectorStorage emptyDense(int length) {
    return new IntLongDenseVectorStorage(length);
  }

  @Override public IntLongVectorStorage emptySparse(int dim, int capacity) {
    return new IntLongSparseVectorStorage(dim, capacity);
  }

  @Override public IntLongVectorStorage emptySorted(int dim, int capacity) {
    return new IntLongSortedVectorStorage(dim, capacity);
  }

  @Override public IntLongVectorStorage emptySparse(int capacity) {
    return new IntLongSparseVectorStorage(dim, capacity);
  }

  @Override public IntLongVectorStorage emptySorted(int capacity) {
    return new IntLongSortedVectorStorage(dim, capacity);
  }

  @Override public IntSet indexIterator() {
    return map.keySet();
  }

  @Override public int size() {
    return map.size();
  }

  @Override public boolean hasKey(int key) {
    return map.containsKey(key);
  }

  @Override public RowType getType() {
    return RowType.T_LONG_SPARSE;
  }

  @Override public boolean isDense() {
    return flag == 1;
  }

  @Override public boolean isSparse() {
    return flag == 2;
  }

  @Override public boolean isSorted() {
    return flag == 4;
  }

  @Override public void clear() {
    map.clear();
  }

  @Override public LongIterator valueIterator() {
    return map.values().iterator();
  }

  @Override public long[] getValues() {
    return map.values().toLongArray();
  }
}
