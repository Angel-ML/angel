package com.tencent.angel.graph.utils;

import it.unimi.dsi.fastutil.ints.IntComparator;

public class LongIndexComparator implements IntComparator {
  private long[] array;
  public LongIndexComparator(long[] array) {
    this.array = array;
  }

  @Override
  public int compare(int i, int i1) {
    if (array[i] == array[i1]) return 0;
    return array[i] < array[i1] ? -1: 1;
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    if (array[o1] == array[o2]) return 0;
    return array[o1] < array[o2] ? -1: 1;
  }
}
