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
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class IntIntSparseVectorStorage implements IntIntVectorStorage {

  private Int2IntOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntIntSparseVectorStorage() {
    super();
  }

  public IntIntSparseVectorStorage(int dim, Int2IntOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public IntIntSparseVectorStorage(int dim, int capacity) {
    this(dim, new Int2IntOpenHashMap(capacity));
  }

  public IntIntSparseVectorStorage(int dim) {
    this(dim, (int) Math.min(64, Math.max(dim, 0)));
  }

  public IntIntSparseVectorStorage(int dim, int[] indices, int[] values) {
    this(dim, new Int2IntOpenHashMap(indices, values));
  }

  @Override
  public int get(int idx) {
    return map.get(idx);
  }

  @Override
  public void set(int idx, int value) {
    map.put(idx, value);
  }

  @Override
  public int[] getIndices() {
    return map.keySet().toIntArray();
  }

  @Override
  public ObjectIterator<Int2IntMap.Entry> entryIterator() {
    return map.int2IntEntrySet().fastIterator();
  }

  @Override
  public IntIntVectorStorage clone() {
    return new IntIntSparseVectorStorage(dim, map.clone());
  }

  @Override
  public IntIntVectorStorage copy() {
    return new IntIntSparseVectorStorage(dim, map.clone());
  }

  @Override
  public IntIntVectorStorage oneLikeDense() {
    int[] oneLikeValues = new int[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntIntDenseVectorStorage(oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSparse() {
    int capacity = map.size();
    int[] oneLikeValues = new int[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntIntSparseVectorStorage(dim, map.keySet().toIntArray(), oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSorted() {
    int capacity = map.size();
    int[] oneLikeValues = new int[capacity];
    int[] indices = map.keySet().toIntArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeDense(int dim) {
    int[] oneLikeValues = new int[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntIntDenseVectorStorage(oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSparse(int dim, int capacity) {
    int[] oneLikeValues = new int[capacity];
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
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSorted(int dim, int capacity) {
    int[] oneLikeValues = new int[capacity];
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
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSparse(int capacity) {
    int[] oneLikeValues = new int[capacity];
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
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSorted(int capacity) {
    int[] oneLikeValues = new int[capacity];
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
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage emptyDense() {
    return new IntIntDenseVectorStorage(dim);
  }

  @Override
  public IntIntVectorStorage emptySparse() {
    return new IntIntSparseVectorStorage(dim, map.size());
  }

  @Override
  public IntIntVectorStorage emptySorted() {
    return new IntIntSortedVectorStorage(dim, map.size());
  }

  @Override
  public IntIntVectorStorage emptyDense(int length) {
    return new IntIntDenseVectorStorage(length);
  }

  @Override
  public IntIntVectorStorage emptySparse(int dim, int capacity) {
    return new IntIntSparseVectorStorage(dim, capacity);
  }

  @Override
  public IntIntVectorStorage emptySorted(int dim, int capacity) {
    return new IntIntSortedVectorStorage(dim, capacity);
  }

  @Override
  public IntIntVectorStorage emptySparse(int capacity) {
    return new IntIntSparseVectorStorage(dim, capacity);
  }

  @Override
  public IntIntVectorStorage emptySorted(int capacity) {
    return new IntIntSortedVectorStorage(dim, capacity);
  }

  @Override
  public IntSet indexIterator() {
    return map.keySet();
  }

  @Override
  public int size() {
    return map.size();
  }

  public void setSize(int size) {
  }

  @Override
  public boolean hasKey(int key) {
    return map.containsKey(key);
  }

  @Override
  public RowType getType() {
    return RowType.T_INT_SPARSE;
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
