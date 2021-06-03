package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public class DynamicLongIntArrayPair extends DynamicArray {

  private List<KeysValues> batchs;
  private KeysValues currentBatch;
  private long[] keys;
  private int[] values;

  class KeysValues {

    private final long[] keys;
    private final int[] values;

    public KeysValues(long[] keys, int[] values) {
      this.keys = keys;
      this.values = values;
    }

    public KeysValues(int size) {
      this(new long[size], new int[size]);
    }

    public void put(int index, long key, int value) {
      keys[index] = key;
      values[index] = value;
    }
  }

  public DynamicLongIntArrayPair(int size) {
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }

    batchs.add(new KeysValues(batchSize));
    currentBatch = batchs.get(0);
  }

  public DynamicLongIntArrayPair() {

  }

  public void add(long key, int value) {
    currentBatch.put(currentBatchPos, key, value);
    currentBatchPos++;

    if (currentBatchPos == batchSize) {
      currentBatchPos = 0;
      currentBatchIndex++;
      if (currentBatchIndex < batchs.size()) {
        currentBatch = batchs.get(currentBatchIndex);
      } else {
        batchs.add(new KeysValues(batchSize));
        currentBatch = batchs.get(currentBatchIndex);
      }
    }
  }

  public List<KeysValues> getBatchs() {
    return batchs;
  }

  public KeysValues getCurrentBatch() {
    return currentBatch;
  }

  public long[] getKeys() {
    if (keys == null) {
      int index = 0;
      keys = new long[size()];
      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex; i++) {
        KeysValues batch = batchs.get(i);
        System.arraycopy(batch.keys, 0, keys, index, batch.keys.length);
        index += batch.keys.length;
      }

      // Last batch
      System.arraycopy(currentBatch.keys, 0, keys, index, currentBatch.keys.length);
    }
    return keys;
  }

  public int[] getValues() {
    if (values == null) {
      int index = 0;
      values = new int[size()];
      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex; i++) {
        KeysValues batch = batchs.get(i);
        System.arraycopy(batch.values, 0, values, index, batch.values.length);
        index += batch.values.length;
      }

      // Last batch
      System.arraycopy(currentBatch.values, 0, values, index, currentBatch.values.length);
    }
    return values;
  }

  @Override
  public void serialize(ByteBuf out) {
    int elemNum = size();

    // Keys
    ByteBufSerdeUtils.serializeInt(out, elemNum);
    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      KeysValues batch = batchs.get(i);
      for (int j = 0; j < batch.keys.length; j++) {
        ByteBufSerdeUtils.serializeLong(out, batch.keys[j]);
        ByteBufSerdeUtils.serializeInt(out, batch.values[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      ByteBufSerdeUtils.serializeLong(out, currentBatch.keys[i]);
      ByteBufSerdeUtils.serializeInt(out, currentBatch.values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    int len = ByteBufSerdeUtils.deserializeInt(in);
    keys = new long[len];
    values = new int[len];
    for (int i = 0; i < len; i++) {
      keys[i] = ByteBufSerdeUtils.deserializeLong(in);
      values[i] = ByteBufSerdeUtils.deserializeInt(in);
    }
  }

  @Override
  public int bufferLen() {
    int elemNum = size();
    return ByteBufSerdeUtils.LONG_LENGTH * elemNum
        + ByteBufSerdeUtils.INT_LENGTH
        + ByteBufSerdeUtils.INT_LENGTH * elemNum;
  }
}

