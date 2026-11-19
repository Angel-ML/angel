package com.tencent.angel.graph.data;

import java.util.HashMap;
import java.util.Map;

public enum NeighborStorageType {
  LONGARRAY((byte) 0),
  INTARRAY((byte) 1);

  static final Map<Byte, NeighborStorageType> indexToOpMap = new HashMap<>();

  static {
    indexToOpMap.put(LONGARRAY.index, LONGARRAY);
    indexToOpMap.put(INTARRAY.index, INTARRAY);
  }

  public static NeighborStorageType valueOf(byte index) {
    return indexToOpMap.get(index);
  }

  private final byte index;

  NeighborStorageType(byte index) {
    this.index = index;
  }

  public byte getIndex() {
    return index;
  }
}
