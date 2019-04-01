package com.tencent.angel.ps.storage.vector.storage;

public enum SerializeArrangement {
  KEY_VALUE(0), VALUE(1);

  private final int value;
  SerializeArrangement(int value) {
    this.value = value;
  }

  public static SerializeArrangement valuesOf(int value) {
    switch (value) {
      case 0: return KEY_VALUE;
      case 1: return VALUE;
      default: return KEY_VALUE;
    }
  }

  public int getValue() {
    return value;
  }
}
