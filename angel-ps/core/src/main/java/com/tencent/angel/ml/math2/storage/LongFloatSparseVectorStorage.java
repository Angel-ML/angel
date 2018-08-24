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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LongFloatSparseVectorStorage implements LongFloatVectorStorage {
  private Long2FloatOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private long dim;

  public LongFloatSparseVectorStorage() {
    super();
  }

  public LongFloatSparseVectorStorage(long dim, Long2FloatOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public LongFloatSparseVectorStorage(long dim, int capacity) {
    this(dim, new Long2FloatOpenHashMap(capacity));
  }

  public LongFloatSparseVectorStorage(long dim) {
    this(dim, Math.max(128, (int) (dim / 1000)));
  }

  public LongFloatSparseVectorStorage(long dim, long[] indices, float[] values) {
    this(dim, new Long2FloatOpenHashMap(indices, values));
  }

  @Override public float get(long idx) {
    return map.get(idx);
  }

  @Override public void set(long idx, float value) {
    map.put(idx, value);
  }

  @Override public long[] getIndices() {
    return map.keySet().toLongArray();
  }

  @Override public ObjectIterator<Long2FloatMap.Entry> entryIterator() {
    return map.long2FloatEntrySet().fastIterator();
  }

  @Override public LongFloatVectorStorage clone() {
    return new LongFloatSparseVectorStorage(dim, map.clone());
  }

  @Override public LongFloatVectorStorage copy() {
    return new LongFloatSparseVectorStorage(dim, map.clone());
  }


  @Override public LongFloatVectorStorage oneLikeSparse() {
    int capacity = map.size();
    float[] oneLikeValues = new float[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongFloatSparseVectorStorage(dim, map.keySet().toLongArray(), oneLikeValues);
  }

  @Override public LongFloatVectorStorage oneLikeSorted() {
    int capacity = map.size();
    float[] oneLikeValues = new float[capacity];
    long[] indices = map.keySet().toLongArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override public LongFloatVectorStorage oneLikeSparse(long dim, int capacity) {
    float[] oneLikeValues = new float[capacity];
    long[] indices = new long[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      long idx = rand.nextInt((int) dim);
      if (!set.contains(idx)) {
        indices[j] = idx;
        set.add(idx);
        j++;
      }
    }
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongFloatVectorStorage oneLikeSorted(long dim, int capacity) {
    float[] oneLikeValues = new float[capacity];
    long[] indices = new long[capacity];
    HashSet set = new HashSet<Integer>();
    Random rand = new Random();
    int j = 0;
    while (set.size() < capacity) {
      long idx = rand.nextInt((int) dim);
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
    return new LongFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongFloatVectorStorage oneLikeSparse(int capacity) {
    float[] oneLikeValues = new float[capacity];
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
    return new LongFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override public LongFloatVectorStorage oneLikeSorted(int capacity) {
    float[] oneLikeValues = new float[capacity];
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
    return new LongFloatSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override public LongFloatVectorStorage emptySparse() {
    return new LongFloatSparseVectorStorage(dim, map.size());
  }

  @Override public LongFloatVectorStorage emptySorted() {
    return new LongFloatSortedVectorStorage(dim, map.size());
  }


  @Override public LongFloatVectorStorage emptySparse(long dim, int capacity) {
    return new LongFloatSparseVectorStorage(dim, capacity);
  }

  @Override public LongFloatVectorStorage emptySorted(long dim, int capacity) {
    return new LongFloatSortedVectorStorage(dim, capacity);
  }

  @Override public LongFloatVectorStorage emptySparse(int capacity) {
    return new LongFloatSparseVectorStorage(dim, capacity);
  }

  @Override public LongFloatVectorStorage emptySorted(int capacity) {
    return new LongFloatSortedVectorStorage(dim, capacity);
  }

  @Override public LongSet indexIterator() {
    return map.keySet();
  }

  @Override public int size() {
    return map.size();
  }

  @Override public boolean hasKey(long key) {
    return map.containsKey(key);
  }

  @Override public RowType getType() {
    return RowType.T_FLOAT_SPARSE_LONGKEY;
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

  @Override public FloatIterator valueIterator() {
    return map.values().iterator();
  }

  @Override public float[] getValues() {
    return map.values().toFloatArray();
  }
}
