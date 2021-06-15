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

public class LongDoubleSortedVectorStorage implements LongDoubleVectorStorage {

  private long[] indices;
  private double[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int size;
  private long dim;

  public LongDoubleSortedVectorStorage() {
    super();
  }

  public LongDoubleSortedVectorStorage(long dim, int size, long[] indices, double[] values) {
    this.flag = 4;
    this.dim = dim;
    this.size = size;
    this.indices = indices;
    this.values = values;
  }

  public LongDoubleSortedVectorStorage(long dim, long[] indices, double[] values) {
    this(dim, indices.length, indices, values);
  }

  public LongDoubleSortedVectorStorage(long dim, int capacity) {
    this(dim, 0, new long[capacity], new double[capacity]);
  }

  public LongDoubleSortedVectorStorage(long dim) {
    this(dim, (int) Math.min(64, Math.max(dim, 0)));
  }

  @Override
  public double get(long idx) {
    if (idx < 0 || idx > dim - 1) {
      throw new ArrayIndexOutOfBoundsException();
    } else if (size == 0 || idx > indices[size - 1] || idx < indices[0]) {
      return 0;
    } else {
      int i = Arrays.binarySearch(indices, idx);
      return i >= 0 ? values[i] : 0;
    }
  }

  @Override
  public void set(long idx, double value) {
    if (idx < 0 || idx > dim - 1) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // 1. find the insert point
    int point;
    if (size == 0 || idx < indices[0]) {
      point = 0;
    } else if (idx > indices[size - 1]) {
      point = size;
    } else {
      point = Arrays.binarySearch(indices, idx);
      if (point >= 0) {
        values[point] = value;
        return;
      } else {
        point = -(point + 1);
      }
    }

    // 2. check the capacity and insert
    if (size == indices.length) {
      long[] newIdxs = new long[(int) (indices.length * 1.5)];
      double[] newValues = new double[(int) (indices.length * 1.5)];
      if (point == 0) {
        System.arraycopy(indices, 0, newIdxs, 1, size);
        System.arraycopy(values, 0, newValues, 1, size);
      } else if (point == size) {
        System.arraycopy(indices, 0, newIdxs, 0, size);
        System.arraycopy(values, 0, newValues, 0, size);
      } else {
        System.arraycopy(indices, 0, newIdxs, 0, point);
        System.arraycopy(values, 0, newValues, 0, point);
        System.arraycopy(indices, point, newIdxs, point + 1, size - point);
        System.arraycopy(values, point, newValues, point + 1, size - point);
      }
      newIdxs[point] = idx;
      newValues[point] = value;
      indices = newIdxs;
      values = newValues;
    } else {
      if (point != size) {
        System.arraycopy(indices, point, indices, point + 1, size - point);
        System.arraycopy(values, point, values, point + 1, size - point);
      }
      indices[point] = idx;
      values[point] = value;
    }

    // 3. increase size
    size++;
  }

  @Override
  public LongDoubleVectorStorage clone() {
    return new LongDoubleSortedVectorStorage(dim, size, ArrayCopy.copy(indices),
        ArrayCopy.copy(values));
  }

  @Override
  public LongDoubleVectorStorage copy() {
    return new LongDoubleSortedVectorStorage(dim, size, ArrayCopy.copy(indices),
        ArrayCopy.copy(values));
  }


  @Override
  public LongDoubleVectorStorage oneLikeSparse() {
    double[] oneLikeValues = new double[size];
    for (int i = 0; i < size; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongDoubleSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongDoubleVectorStorage oneLikeSorted() {
    double[] oneLikeValues = new double[size];
    for (int i = 0; i < size; i++) {
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
  public LongDoubleVectorStorage oneLikeSorted(long dim, int capacity) {
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
    return new LongDoubleSparseVectorStorage(dim, indices.length);
  }

  @Override
  public LongDoubleVectorStorage emptySorted() {
    return new LongDoubleSortedVectorStorage(dim, indices.length);
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
  public long[] getIndices() {
    return indices;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean hasKey(long key) {
    return (size != 0 && key <= indices[size - 1] && key >= indices[0]
        && Arrays.binarySearch(indices, key) > 0);
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
    Arrays.parallelSetAll(indices, (int value) -> 0);
    Arrays.parallelSetAll(values, (int value) -> 0);
  }

  @Override
  public double[] getValues() {
    return values;
  }
}
