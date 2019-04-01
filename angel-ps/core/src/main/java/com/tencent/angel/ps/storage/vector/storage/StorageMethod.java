package com.tencent.angel.ps.storage.vector.storage;

public enum StorageMethod {
  DENSE(0), SPARSE(1), SORTED(2);

  private final int value;
  StorageMethod(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static StorageMethod valuesOf(int value) {
    switch (value) {
      case 0: return DENSE;
      case 1: return SPARSE;
      case 2: return SORTED;
      default: return SPARSE;
    }
  }
}
