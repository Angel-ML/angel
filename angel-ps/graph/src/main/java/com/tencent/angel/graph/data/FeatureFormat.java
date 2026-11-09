package com.tencent.angel.graph.data;

import java.util.HashMap;
import java.util.Map;

public enum FeatureFormat {
  DENSE(0),
  SPARSE(1),
  DENSE_SPARSE(2),
  DENSE_HIGH_SPARSE(3),
  DENSE_HIGH_SPARSE_SEQUENCE(4);

  static final Map<Integer, FeatureFormat> indexToOpMap = new HashMap<>();

  static {
    indexToOpMap.put(DENSE.index, DENSE);
    indexToOpMap.put(SPARSE.index, SPARSE);
    indexToOpMap.put(DENSE_SPARSE.index, DENSE_SPARSE);
    indexToOpMap.put(DENSE_HIGH_SPARSE.index, DENSE_HIGH_SPARSE);
    indexToOpMap.put(DENSE_HIGH_SPARSE_SEQUENCE.index, DENSE_HIGH_SPARSE_SEQUENCE);
  }

  public static FeatureFormat valueOf(int index) {
    return indexToOpMap.get(index);
  }

  private final int index;

  FeatureFormat(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }
}