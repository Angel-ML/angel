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

public class IntDoubleDenseVectorStorage implements IntDoubleVectorStorage {

  private double[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntDoubleDenseVectorStorage() {
    super();
  }

  public IntDoubleDenseVectorStorage(double[] values) {
    this.flag = 1;
    this.dim = values.length;
    this.values = values;
  }

  public IntDoubleDenseVectorStorage(int dim) {
    this(new double[dim]);
  }

  @Override
  public double get(int idx) {
    return values[idx];
  }

  @Override
  public void set(int idx, double value) {
    values[idx] = value;
  }

  @Override
  public double[] getValues() {
    return values;
  }

  @Override
  public IntDoubleVectorStorage clone() {
    return new IntDoubleDenseVectorStorage(ArrayCopy.copy(values));
  }

  @Override
  public IntDoubleVectorStorage copy() {
    return new IntDoubleDenseVectorStorage(ArrayCopy.copy(values));
  }

  @Override
  public IntDoubleVectorStorage oneLikeDense() {
    double[] oneLikeValues = new double[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntDoubleDenseVectorStorage(oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeSparse() {
    int capacity = Math.max(128, (int) (dim / 1000));
    double[] oneLikeValues = new double[capacity];
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
    return new IntDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeSorted() {
    int capacity = Math.max(128, (int) (dim / 1000));
    double[] oneLikeValues = new double[capacity];
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
    return new IntDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeDense(int dim) {
    double[] oneLikeValues = new double[dim];
    for (int i = 0; i < dim; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntDoubleDenseVectorStorage(oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeSparse(int dim, int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new IntDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeSorted(int dim, int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new IntDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeSparse(int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new IntDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage oneLikeSorted(int capacity) {
    double[] oneLikeValues = new double[capacity];
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
    return new IntDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntDoubleVectorStorage emptyDense() {
    return new IntDoubleDenseVectorStorage(dim);
  }

  @Override
  public IntDoubleVectorStorage emptySparse() {
    return new IntDoubleSparseVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
  }

  @Override
  public IntDoubleVectorStorage emptySorted() {
    return new IntDoubleSortedVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
  }

  @Override
  public IntDoubleVectorStorage emptyDense(int length) {
    return new IntDoubleDenseVectorStorage(length);
  }

  @Override
  public IntDoubleVectorStorage emptySparse(int dim, int capacity) {
    return new IntDoubleSparseVectorStorage(dim, capacity);
  }

  @Override
  public IntDoubleVectorStorage emptySorted(int dim, int capacity) {
    return new IntDoubleSortedVectorStorage(dim, capacity);
  }

  @Override
  public IntDoubleVectorStorage emptySparse(int capacity) {
    return new IntDoubleSparseVectorStorage(dim, capacity);
  }

  @Override
  public IntDoubleVectorStorage emptySorted(int capacity) {
    return new IntDoubleSortedVectorStorage(dim, capacity);
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public boolean hasKey(int key) {
    return (key >= 0 && key < values.length);
  }

  @Override
  public RowType getType() {
    return RowType.T_DOUBLE_DENSE;
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
    Arrays.parallelSetAll(values, (int value) -> 0);
  }
}
