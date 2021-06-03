package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public class DynamicLongArray extends DynamicArray {

  private List<long[]> batchs;
  private long[] currentBatch;
  private long[] data;

  public DynamicLongArray(int size) {
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }
    batchs.add(new long[batchSize]);
    currentBatch = batchs.get(0);
  }

  public DynamicLongArray() {
  }

  public void add(long value) {
    currentBatch[currentBatchPos] = value;
    currentBatchPos++;

    if (currentBatchPos == batchSize) {
      currentBatchPos = 0;
      currentBatchIndex++;
      if (currentBatchIndex < batchs.size()) {
        currentBatch = batchs.get(currentBatchIndex);
      } else {
        batchs.add(new long[batchSize]);
        currentBatch = batchs.get(currentBatchIndex);
      }
    }
  }

  public List<long[]> getBatchs() {
    return batchs;
  }

  public long[] getCurrentBatch() {
    return currentBatch;
  }

  public long[] getData() {
    if (data == null) {
      int index = 0;
      data = new long[size()];
      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex; i++) {
        long[] batch = batchs.get(i);
        for (int j = 0; j < batch.length; j++) {
          data[index++] = batch[j];
        }
      }

      // Last batch
      for (int i = 0; i < currentBatchPos; i++) {
        data[index++] = currentBatch[i];
      }
    }

    return data;
  }

  @Override
  public void serialize(ByteBuf out) {
    ByteBufSerdeUtils.serializeInt(out, batchSize * currentBatchIndex + currentBatchPos);

    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      long[] batch = batchs.get(i);
      for (int j = 0; j < batch.length; j++) {
        ByteBufSerdeUtils.serializeLong(out, batch[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      ByteBufSerdeUtils.serializeLong(out, currentBatch[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    data = ByteBufSerdeUtils.deserializeLongs(in);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.LONG_LENGTH * (batchSize * currentBatchIndex + currentBatchPos);
  }
}
