package com.tencent.angel.ml.math2.storage;

import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.matrix.RowType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class IntFloatSortedVectorStorage implements IntFloatVectorStorage {

  private int[] indices;
  private float[] values;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private int size;
  private int dim;

  public IntFloatSortedVectorStorage() {
    super();
  }

  public IntFloatSortedVectorStorage(int dim, int size, int[] indices, float[] values) {
    this.flag = 4;
    this.dim = dim;
    this.size = size;
    this.indices = indices;
    this.values = values;
  }

  public IntFloatSortedVectorStorage(int dim, int[] indices, float[] values) {
    this(dim, indices.length, indices, values);
  }

  public IntFloatSortedVectorStorage(int dim, int capacity) {
    this(dim, 0, new int[capacity], new float[capacity]);
  }

  public IntFloatSortedVectorStorage(int dim) {
    this(dim, Math.min(64, Math.max(dim, 0)));
  }

  @Override
  public float get(int idx) {
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
  public void set(int idx, float value) {
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
      int[] newIdxs = new int[(int) (indices.length * 1.5)];
      float[] newValues = new float[(int) (indices.length * 1.5)];
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
  public IntFloatVectorStorage clone() {
    return new IntFloatSortedVectorStorage(dim, size, ArrayCopy.copy(indices),
        ArrayCopy.copy(values));
  }

  @Override
  public IntFloatVectorStorage copy() {
    return new IntFloatSortedVectorStorage(dim, size, ArrayCopy.copy(indices),
        ArrayCopy.copy(values));
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
    float[] oneLikeValues = new float[size];
    for (int i = 0; i < size; i++) {
      oneLikeValues[i] = 1;
    }
    return new IntFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public IntFloatVectorStorage oneLikeSorted() {
    float[] oneLikeValues = new float[size];
    for (int i = 0; i < size; i++) {
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
    return new IntFloatSparseVectorStorage(dim, indices.length);
  }

  @Override
  public IntFloatVectorStorage emptySorted() {
    return new IntFloatSortedVectorStorage(dim, indices.length);
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
  public int[] getIndices() {
    return indices;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean hasKey(int key) {
    return (size != 0 && key <= indices[size - 1] && key >= indices[0]
        && Arrays.binarySearch(indices, key) > 0);
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
    Arrays.parallelSetAll(indices, (int value) -> 0);
    for (int i = 0; i < values.length; i++) {
      values[i] = 0;
    }
  }

  @Override
  public float[] getValues() {
    return values;
  }
}
