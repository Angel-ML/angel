package com.tencent.angel.ml.math2.storage;

import com.tencent.angel.ml.matrix.RowType;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class LongLongSparseVectorStorage implements LongLongVectorStorage {

  private Long2LongOpenHashMap map;
  private byte flag; // 001: dense; 010: sparse; 100: sorted
  private long dim;

  public LongLongSparseVectorStorage() {
    super();
  }

  public LongLongSparseVectorStorage(long dim, Long2LongOpenHashMap map) {
    this.flag = 2;
    this.dim = dim;
    this.map = map;
  }

  public LongLongSparseVectorStorage(long dim, int capacity) {
    this(dim, new Long2LongOpenHashMap(capacity));
  }

  public LongLongSparseVectorStorage(long dim) {
    this(dim, (int) Math.min(64, Math.max(dim, 0)));
  }

  public LongLongSparseVectorStorage(long dim, long[] indices, long[] values) {
    this(dim, new Long2LongOpenHashMap(indices, values));
  }

  @Override
  public long get(long idx) {
    return map.get(idx);
  }

  @Override
  public void set(long idx, long value) {
    map.put(idx, value);
  }

  @Override
  public long[] getIndices() {
    return map.keySet().toLongArray();
  }

  @Override
  public ObjectIterator<Long2LongMap.Entry> entryIterator() {
    return map.long2LongEntrySet().fastIterator();
  }

  @Override
  public LongLongVectorStorage clone() {
    return new LongLongSparseVectorStorage(dim, map.clone());
  }

  @Override
  public LongLongVectorStorage copy() {
    return new LongLongSparseVectorStorage(dim, map.clone());
  }


  @Override
  public LongLongVectorStorage oneLikeSparse() {
    int capacity = map.size();
    long[] oneLikeValues = new long[capacity];

    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongLongSparseVectorStorage(dim, map.keySet().toLongArray(), oneLikeValues);
  }

  @Override
  public LongLongVectorStorage oneLikeSorted() {
    int capacity = map.size();
    long[] oneLikeValues = new long[capacity];
    long[] indices = map.keySet().toLongArray();
    Arrays.sort(indices);
    for (int i = 0; i < capacity; i++) {
      oneLikeValues[i] = 1;
    }
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }


  @Override
  public LongLongVectorStorage oneLikeSparse(long dim, int capacity) {
    long[] oneLikeValues = new long[capacity];
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
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongLongVectorStorage oneLikeSorted(long dim, int capacity) {
    long[] oneLikeValues = new long[capacity];
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
    return new LongLongSparseVectorStorage(dim, indices, oneLikeValues);
  }

  @Override
  public LongLongVectorStorage oneLikeSparse(int capacity) {
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

  @Override
  public LongLongVectorStorage oneLikeSorted(int capacity) {
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


  @Override
  public LongLongVectorStorage emptySparse() {
    return new LongLongSparseVectorStorage(dim, map.size());
  }

  @Override
  public LongLongVectorStorage emptySorted() {
    return new LongLongSortedVectorStorage(dim, map.size());
  }


  @Override
  public LongLongVectorStorage emptySparse(long dim, int capacity) {
    return new LongLongSparseVectorStorage(dim, capacity);
  }

  @Override
  public LongLongVectorStorage emptySorted(long dim, int capacity) {
    return new LongLongSortedVectorStorage(dim, capacity);
  }

  @Override
  public LongLongVectorStorage emptySparse(int capacity) {
    return new LongLongSparseVectorStorage(dim, capacity);
  }

  @Override
  public LongLongVectorStorage emptySorted(int capacity) {
    return new LongLongSortedVectorStorage(dim, capacity);
  }

  @Override
  public LongSet indexIterator() {
    return map.keySet();
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean hasKey(long key) {
    return map.containsKey(key);
  }

  @Override
  public RowType getType() {
    return RowType.T_LONG_SPARSE_LONGKEY;
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
  public LongIterator valueIterator() {
    return map.values().iterator();
  }

  @Override
  public long[] getValues() {
    return map.values().toLongArray();
  }
}
