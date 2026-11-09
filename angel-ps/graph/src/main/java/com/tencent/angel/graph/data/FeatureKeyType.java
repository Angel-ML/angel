package com.tencent.angel.graph.data;

import java.util.HashMap;
import java.util.Map;

public enum FeatureKeyType {
  LONG((byte) 0),
  INT((byte) 1);

  static final Map<Byte, FeatureKeyType> indexToOpMap = new HashMap<>();

  static {
    indexToOpMap.put(LONG.index, LONG);
    indexToOpMap.put(INT.index, INT);
  }

  public static FeatureKeyType valueOf(byte index) {
    return indexToOpMap.get(index);
  }

  private final byte index;

  FeatureKeyType(byte index) {
    this.index = index;
  }

  public byte getIndex() {
    return index;
  }
}
