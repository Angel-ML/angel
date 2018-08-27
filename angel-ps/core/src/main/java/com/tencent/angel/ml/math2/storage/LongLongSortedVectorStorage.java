package com.tencent.angel.ml.math2.storage;

import java.util.Arrays;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.math2.utils.ArrayCopy;

import java.util.Arrays;
import java.util.Random;
import java.util.HashSet;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LongLongSortedVectorStorage implements LongLongVectorStorage {
  private long[] indices;
  private long[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int size;
  private long dim;

  public LongLongSortedVectorStorage() {
    super();
  }

  public LongLongSortedVectorStorage(long dim, int size, long[] indices, long[] values) {
    this.flag = 4;
    this.dim = dim;
    this.size = size;
    this.indices = indices;
    this.values = values;
  }

  public LongLongSortedVectorStorage(long dim, long[] indices, long[] values) {
    this(dim, indices.length, indices, values);
  }

  public LongLongSortedVectorStorage(long dim, int capacity) {
    this(dim, 0, new long[capacity], new long[capacity]);
  }

  public LongLongSortedVectorStorage(long dim) {
    this(dim, Math.max(128, (int) (dim / 1000)));
  }

  @Override public long get(long idx) {
    if (idx < 0 || idx > dim - 1) {
      throw new ArrayIndexOutOfBoundsException();
    } else if (size == 0 || idx > indices[size - 1] || idx < indices[0]) {
      return 0;
    } else {
      int i = Arrays.binarySearch(indices, idx);
      return i >= 0 ? values[i] : 0;
    }
  }

  @Override public void set(long idx, long value) {
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
      long[] newValues = new long[(int) (indices.length * 1.5)];
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

  @Override public LongLongVectorStorage clone() {
    return new LongLongSortedVectorStorage(dim, size, ArrayCopy.copy(indices),
      ArrayCopy.copy(values));
  }

  @Override public LongLongVectorStorage copy() {
    return new LongLongSortedVectorStorage(dim, size, ArrayCopy.copy(indices),
      ArrayCopy.copy(values));
  }


  @Override public LongLongVectorStorage oneLikeSparse() {
    long[] oneLikeValues = new long[size];
    for (int i = 0; i < size; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongLongVectorStorage oneLikeSorted() {
    long[] oneLikeValues = new long[size];
    for (int i = 0; i < size; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override public LongLongVectorStorage oneLikeSparse(long dim, int capacity) {
    long[] oneLikeValues = new long[capacity];
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
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongLongVectorStorage oneLikeSorted(long dim, int capacity) {
    long[] oneLikeValues = new long[capacity];
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
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongLongVectorStorage oneLikeSparse(int capacity) {
    long[] oneLikeValues = new long[capacity];
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
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongLongVectorStorage oneLikeSorted(int capacity) {
    long[] oneLikeValues = new long[capacity];
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
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override public LongLongVectorStorage emptySparse() {
    return new LongLongSparseVectorStorage(dim, indices.length);
  }

  @Override public LongLongVectorStorage emptySorted() {
    return new LongLongSortedVectorStorage(dim, indices.length);
  }


  @Override public LongLongVectorStorage emptySparse(long dim, int capacity) {
    return new LongLongSparseVectorStorage(dim, capacity);
  }

  @Override public LongLongVectorStorage emptySorted(long dim, int capacity) {
    return new LongLongSortedVectorStorage(dim, capacity);
  }

  @Override public LongLongVectorStorage emptySparse(int capacity) {
    return new LongLongSparseVectorStorage(dim, capacity);
  }

  @Override public LongLongVectorStorage emptySorted(int capacity) {
    return new LongLongSortedVectorStorage(dim, capacity);
  }

  @Override public long[] getIndices() {
    return indices;
  }

  @Override public int size() {
    return size;
  }

  @Override public boolean hasKey(long key) {
    return (size != 0 && key <= indices[size - 1] && key >= indices[0]
      && Arrays.binarySearch(indices, key) > 0);
  }

  @Override public RowType getType() {
    return RowType.T_LONG_SPARSE_LONGKEY;
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
    Arrays.parallelSetAll(indices, (int value) -> 0);
    Arrays.parallelSetAll(values, (int value) -> 0);
  }

  @Override public long[] getValues() {
    return values;
  }
}
