package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public class DynamicIntDoubleArrayPair extends DynamicArray {

  private List<KeysValues> batchs;
  private KeysValues currentBatch;
  private int[] keys;
  private double[] values;

  class KeysValues {

    private final int[] keys;
    private final double[] values;

    public KeysValues(int[] keys, double[] values) {
      this.keys = keys;
      this.values = values;
    }

    public KeysValues(int size) {
      this(new int[size], new double[size]);
    }

    public void put(int index, int key, double value) {
      keys[index] = key;
      values[index] = value;
    }
  }

  public DynamicIntDoubleArrayPair(int size) {
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }

    batchs.add(new KeysValues(batchSize));
    currentBatch = batchs.get(0);
  }

  public DynamicIntDoubleArrayPair() {

  }

  public void add(int key, double value) {
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

  public int[] getKeys() {
    if (keys == null) {
      int index = 0;
      keys = new int[size()];
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

  public double[] getValues() {
    if (values == null) {
      int index = 0;
      values = new double[size()];
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
        ByteBufSerdeUtils.serializeInt(out, batch.keys[j]);
        ByteBufSerdeUtils.serializeDouble(out, batch.values[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      ByteBufSerdeUtils.serializeInt(out, currentBatch.keys[i]);
      ByteBufSerdeUtils.serializeDouble(out, currentBatch.values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    int len = ByteBufSerdeUtils.deserializeInt(in);
    keys = new int[len];
    values = new double[len];
    for (int i = 0; i < len; i++) {
      keys[i] = ByteBufSerdeUtils.deserializeInt(in);
      values[i] = ByteBufSerdeUtils.deserializeDouble(in);
    }
  }

  @Override
  public int bufferLen() {
    int elemNum = size();
    return ByteBufSerdeUtils.INT_LENGTH * (elemNum + 1)
        + ByteBufSerdeUtils.DOUBLE_LENGTH * elemNum;
  }
}
