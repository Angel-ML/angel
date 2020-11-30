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
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class IntFloatSparseVectorStorage implements IntFloatVectorStorage {

  private Int2FloatOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntFloatSparseVectorStorage() {
    super();
  }

  public IntFloatSparseVectorStorage(int dim, Int2FloatOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public IntFloatSparseVectorStorage(int dim, int capacity) {
    this(dim, new Int2FloatOpenHashMap(capacity));
  }

  public IntFloatSparseVectorStorage(int dim) {
    this(dim, (int) Math.min(64, Math.max(dim, 0)));
  }

  public IntFloatSparseVectorStorage(int dim, int[] indices, float[] values) {
    this(dim, new Int2FloatOpenHashMap(indices, values));
  }

  @Override
  public float get(int idx) {
    return map.get(idx);
  }

  @Override
  public void set(int idx, float value) {
    map.put(idx, value);
  }

  @Override
  public int[] getIndices() {
    return map.keySet().toIntArray();
  }

  @Override
  public ObjectIterator<Int2FloatMap.Entry> entryIterator() {
    return map.int2FloatEntrySet().fastIterator();
  }

  @Override
  public IntFloatVectorStorage clone() {
    return new IntFloatSparseVectorStorage(dim, map.clone());
  }

  @Override
  public IntFloatVectorStorage copy() {
    return new IntFloatSparseVectorStorage(dim, map.clone());
  }

  @Override
  public IntFloatVectorStorage oneLikeDense() {
    float[] oneLikeValues = new float[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntFloatDenseVectorStorage(oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSparse() {
    int capacity = map.size();
    float[] oneLikeValues = new float[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntFloatSparseVectorStorage(dim, map.keySet().toIntArray(), oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSorted() {
    int capacity = map.size();
    float[] oneLikeValues = new float[capacity];
    int[] indices = map.keySet().toIntArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeDense(int dim) {
    float[] oneLikeValues = new float[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntFloatDenseVectorStorage(oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSparse(int dim, int capacity) {
    float[] oneLikeValues = new float[capacity];
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
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSorted(int dim, int capacity) {
    float[] oneLikeValues = new float[capacity];
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
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSparse(int capacity) {
    float[] oneLikeValues = new float[capacity];
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
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSorted(int capacity) {
    float[] oneLikeValues = new float[capacity];
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
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage emptyDense() {
    return new IntFloatDenseVectorStorage(dim);
  }

  @Override
  public IntFloatVectorStorage emptySparse() {
    return new IntFloatSparseVectorStorage(dim, map.size());
  }

  @Override
  public IntFloatVectorStorage emptySorted() {
    return new IntFloatSortedVectorStorage(dim, map.size());
  }

  @Override
  public IntFloatVectorStorage emptyDense(int length) {
    return new IntFloatDenseVectorStorage(length);
  }

  @Override
  public IntFloatVectorStorage emptySparse(int dim, int capacity) {
    return new IntFloatSparseVectorStorage(dim, capacity);
  }

  @Override
  public IntFloatVectorStorage emptySorted(int dim, int capacity) {
    return new IntFloatSortedVectorStorage(dim, capacity);
  }

  @Override
  public IntFloatVectorStorage emptySparse(int capacity) {
    return new IntFloatSparseVectorStorage(dim, capacity);
  }

  @Override
  public IntFloatVectorStorage emptySorted(int capacity) {
    return new IntFloatSortedVectorStorage(dim, capacity);
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
    return RowType.T_FLOAT_SPARSE;
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
  public FloatIterator valueIterator() {
    return map.values().iterator();
  }

  @Override
  public float[] getValues() {
    return map.values().toFloatArray();
  }
}
