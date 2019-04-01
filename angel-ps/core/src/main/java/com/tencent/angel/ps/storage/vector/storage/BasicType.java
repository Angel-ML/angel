package com.tencent.angel.ps.storage.vector.storage;

public enum BasicType {
  INT(0), LONG(1), FLOAT(2), DOUBLE(3);

  private final int value;
  BasicType(int value) {
    this.value = value;
  }

  public static BasicType valuesOf(int value) {
    switch (value) {
      case 0: return INT;
      case 1: return LONG;
      case 2: return FLOAT;
      case 3: return DOUBLE;
      default: return INT;
    }
  }

  public int getValue() {
    return value;
  }
}
