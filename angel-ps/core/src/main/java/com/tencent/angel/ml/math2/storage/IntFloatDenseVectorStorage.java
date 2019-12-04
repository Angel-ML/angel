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

import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.matrix.RowType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class IntFloatDenseVectorStorage implements IntFloatVectorStorage {

  private float[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntFloatDenseVectorStorage() {
    super();
  }

  public IntFloatDenseVectorStorage(float[] values) {
    this.flag = 1;
    this.dim = values.length;
    this.values = values;
  }

  public IntFloatDenseVectorStorage(int dim) {
    this(new float[dim]);
  }

  @Override
  public float get(int idx) {
    return values[idx];
  }

  @Override
  public void set(int idx, float value) {
    values[idx] = value;
  }

  @Override
  public float[] getValues() {
    return values;
  }

  @Override
  public IntFloatVectorStorage clone() {
    return new IntFloatDenseVectorStorage(ArrayCopy.copy(values));
  }

  @Override
  public IntFloatVectorStorage copy() {
    return new IntFloatDenseVectorStorage(ArrayCopy.copy(values));
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
    int capacity = Math.max(128, (int) (dim / 1000));
    float[] oneLikeValues = new float[capacity];
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
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSorted() {
    int capacity = Math.max(128, (int) (dim / 1000));
    float[] oneLikeValues = new float[capacity];
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
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage emptyDense() {
    return new IntFloatDenseVectorStorage(dim);
  }

  @Override
  public IntFloatVectorStorage emptySparse() {
    return new IntFloatSparseVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
  }

  @Override
  public IntFloatVectorStorage emptySorted() {
    return new IntFloatSortedVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
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
  public int size() {
    return values.length;
  }

  public void setSize(int size) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public boolean hasKey(int key) {
    return (key >= 0 && key < values.length);
  }

  @Override
  public RowType getType() {
    return RowType.T_FLOAT_DENSE;
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
    for (int i = 0; i < values.length; i++) {
      values[i] = 0;
    }
  }
}
