package com.tencent.angel.ml.math2.storage;

import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.matrix.RowType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class IntIntDenseVectorStorage implements IntIntVectorStorage {

  private int[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int dim;

  public IntIntDenseVectorStorage() {
    super();
  }

  public IntIntDenseVectorStorage(int[] values) {
    this.flag = 1;
    this.dim = values.length;
    this.values = values;
  }

  public IntIntDenseVectorStorage(int dim) {
    this(new int[dim]);
  }

  @Override
  public int get(int idx) {
    return values[idx];
  }

  @Override
  public void set(int idx, int value) {
    values[idx] = value;
  }

  @Override
  public int[] getValues() {
    return values;
  }

  @Override
  public IntIntVectorStorage clone() {
    return new IntIntDenseVectorStorage(ArrayCopy.copy(values));
  }

  @Override
  public IntIntVectorStorage copy() {
    return new IntIntDenseVectorStorage(ArrayCopy.copy(values));
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
    int capacity = Math.max(128, (int) (dim / 1000));
    int[] oneLikeValues = new int[capacity];
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
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage oneLikeSorted() {
    int capacity = Math.max(128, (int) (dim / 1000));
    int[] oneLikeValues = new int[capacity];
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
    return new IntIntSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntIntVectorStorage emptyDense() {
    return new IntIntDenseVectorStorage(dim);
  }

  @Override
  public IntIntVectorStorage emptySparse() {
    return new IntIntSparseVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
  }

  @Override
  public IntIntVectorStorage emptySorted() {
    return new IntIntSortedVectorStorage(dim, Math.max(128, (int) (dim / 1000)));
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
  public int size() {
    return values.length;
  }

  @Override
  public boolean hasKey(int key) {
    return (key >= 0 && key < values.length);
  }

  @Override
  public RowType getType() {
    return RowType.T_INT_DENSE;
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
