package com.tencent.angel.common.collections;

import com.tencent.angel.common.Serialize;

public abstract class DynamicArray implements Serialize {

  public static final String defaultValueClass = DummyElement.class.getName();
  protected final int defaultBatchNum = 16;
  protected int batchSize = 64;
  protected int currentBatchIndex;
  protected int currentBatchPos;
  /** Element count restored by deserialize(); -1 means the array was built via add(). */
  protected int deserializedSize = -1;

  public int size() {
    if (deserializedSize >= 0) {
      return deserializedSize;
    }
    return currentBatchIndex * batchSize + currentBatchPos;
  }

  public void setBatchSize(int b) {
    batchSize = b;
  }
}
