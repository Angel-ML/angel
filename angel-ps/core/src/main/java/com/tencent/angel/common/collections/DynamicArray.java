package com.tencent.angel.common.collections;

import com.tencent.angel.common.Serialize;

public abstract class DynamicArray implements Serialize {

  public static final String defaultValueClass = DummyElement.class.getName();
  protected final int defaultBatchNum = 16;
  protected final int batchSize = 64;
  protected int currentBatchIndex;
  protected int currentBatchPos;

  public int size() {
    return currentBatchIndex * batchSize + currentBatchPos;
  }
}
