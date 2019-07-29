package com.tencent.angel.graph.utils;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;

public class LongArrayListIndexComparator implements IntComparator {
  private LongArrayList array;
  public LongArrayListIndexComparator(LongArrayList array) {
    this.array = array;
  }

  @Override
  public int compare(int i, int i1) {
    if (array.getLong(i) == array.getLong(i1)) return 0;
    return array.getLong(i) < array.getLong(i1) ? -1 : 1;
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    if (array.getLong(o1) == array.getLong(o2)) return 0;
    return array.getLong(o1) < array.getLong(o2) ? -1 : 1;
  }
}
