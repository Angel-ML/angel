package com.tencent.angel.graph.client.psf.sample.sampleneighbor;


import java.util.HashMap;
import java.util.Map;

public enum SampleMethod {
  RANDOM(0),
  ALIASTABLE(1),
  SIMPLEREJECT(2);

  static final Map<Integer, SampleMethod> indexToOpMap = new HashMap<>();

  static {
    indexToOpMap.put(RANDOM.index, RANDOM);
    indexToOpMap.put(ALIASTABLE.index, ALIASTABLE);
    indexToOpMap.put(SIMPLEREJECT.index, SIMPLEREJECT);
  }

  private final int index;

  SampleMethod(int index) {
    this.index = index;
  }

  public static SampleMethod valueOf(int index) {
    return indexToOpMap.get(index);
  }

  public int getIndex() {
    return index;
  }
}
