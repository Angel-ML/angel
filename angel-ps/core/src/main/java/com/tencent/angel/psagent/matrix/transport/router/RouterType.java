package com.tencent.angel.psagent.matrix.transport.router;

import java.util.HashMap;
import java.util.Map;

public enum RouterType {
  RANGE(1), HASH(2);

  public static Map<Integer, RouterType> typeIdToTypeMap;

  static {
    typeIdToTypeMap = new HashMap<>();
    typeIdToTypeMap.put(RANGE.typeId, RANGE);
    typeIdToTypeMap.put(HASH.typeId, HASH);
  }

  public static RouterType valueOf(int id) {
    return typeIdToTypeMap.get(id);
  }

  private final int typeId;

  RouterType(int typeId) {
    this.typeId = typeId;
  }

  public int getTypeId() {
    return typeId;
  }
}
