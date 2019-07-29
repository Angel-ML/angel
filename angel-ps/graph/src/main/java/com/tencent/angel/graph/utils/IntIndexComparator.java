package com.tencent.angel.graph.utils;

import it.unimi.dsi.fastutil.ints.IntComparator;

public class IntIndexComparator implements IntComparator {
  private int[] array;
  public IntIndexComparator(int[] array) {
    this.array = array;
  }

  @Override
  public int compare(int i, int i1) {
    return array[i] - array[i1];
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    return array[o1] - array[o2];
  }
}
