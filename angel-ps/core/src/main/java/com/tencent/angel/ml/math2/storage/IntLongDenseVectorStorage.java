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
import com.tencent.angel.ml.math2.utils.ArrayCopy;

import java.util.Arrays;
import java.util.Random;
import java.util.HashSet;

public class IntLongDenseVectorStorage implements IntLongVectorStorage {
  private long[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntLongDenseVectorStorage(long[] values) {
    this.flag = 1;
    this.dim = values.length;
    this.values = values;
  }

  public IntLongDenseVectorStorage(int dim) {
    this(new long[dim]);
  }

  @Override public long get(int idx) {
    return values[idx];
  }

  @Override public void set(int idx, long value) {
    values[idx] = value;
  }

  @Override public long[] getValues() {
    return values;
  }

  @Override public IntLongVectorStorage clone() {
    return new IntLongDenseVectorStorage(ArrayCopy.copy(values));
  }

  @Override public IntLongVectorStorage copy() {
    return new IntLongDenseVectorStorage(ArrayCopy.copy(values));
  }

  @Override public IntLongVectorStorage oneLikeDense() {
    long[] oneLikeValues = new long[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntLongDenseVectorStorage(oneLikeValues);
  }

  @Override public IntLongVectorStorage oneLikeSparse() {
    int capacity = Math.max(128, (int) (dim / 1000));
    long[] oneLikeValues = new long[capacity];
    int[] indices = new int[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      int idx = rand.nextInt(dim);
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

  @Override public IntLongVectorStorage oneLikeSorted() {
    int capacity = Math.max(128, (int) (dim / 1000));
    long[] oneLikeValues = new long[capacity];
    int[] indices = new int[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      int idx = rand.nextInt(dim);
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
      int idx = rand.nextInt(dim);
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
      int idx = rand.nextInt(dim);
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
      int idx = rand.nextInt(dim);
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
      int idx = rand.nextInt(dim);
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
    return new IntLongSparseVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
  }

  @Override public IntLongVectorStorage emptySorted() {
    return new IntLongSortedVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
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

  @Override public int size() {
    return values.length;
  }

  @Override public boolean hasKey(int key) {
    return (key >= 0 && key < values.length);
  }

  @Override public RowType getType() {
    return RowType.T_LONG_DENSE;
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
    Arrays.parallelSetAll(values, (int value) -> 0);
  }
}
