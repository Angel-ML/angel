package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.twitter.chill.ScalaKryoInstantiator;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompressedDynamicLongArray extends DynamicArray {

  private List<byte[]> batchs;
  private long[] currentBatch;
  private long[] data;

  public CompressedDynamicLongArray(int size, int b) {
    setBatchSize(b);
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }
    currentBatch = new long[batchSize];
  }

  public CompressedDynamicLongArray() {
  }

  public void add(long value) {
    currentBatch[currentBatchPos] = value;
    currentBatchPos++;

    if (currentBatchPos == batchSize) {
      currentBatchPos = 0;
      currentBatchIndex++;
      batchs.add(ScalaKryoInstantiator.defaultPool().toBytesWithoutClass(currentBatch));
      //      currentBatch = new long[batchSize];
    }
  }

  public List<byte[]> getBatchs() {
    return batchs;
  }

  public int getCurrentBatchPos() {
    return currentBatchPos;
  }

  public int getBatchSize() {
    return batchSize;
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
        long[] batch = ScalaKryoInstantiator.defaultPool().fromBytes(batchs.get(i), long[].class);
        for (int j = 0; j < batch.length; j++) {
          data[index++] = batch[j];
        }
      }
      batchs.clear();

      // Last batch
      for (int i = 0; i < currentBatchPos; i++) {
        data[index++] = currentBatch[i];
      }
      currentBatch = null;
    }

    return data;
  }

  @Override
  public void serialize(ByteBuf out) {
    if (data == null) {
      ByteBufSerdeUtils.serializeInt(out, batchSize * currentBatchIndex + currentBatchPos);

      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex; i++) {
        long [] batch = ScalaKryoInstantiator.defaultPool().fromBytes(batchs.get(i), long[].class);
        for (int j = 0; j < batch.length; j++) {
          ByteBufSerdeUtils.serializeLong(out, batch[j]);
        }
      }
      // Last batch
      for (int i = 0; i < currentBatchPos; i++) {
        ByteBufSerdeUtils.serializeLong(out, currentBatch[i]);
      }
    } else {
      ByteBufSerdeUtils.serializeLongs(out, data);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    data = ByteBufSerdeUtils.deserializeLongs(in);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.LONG_LENGTH * (batchSize * currentBatchIndex + currentBatchPos) + 4;
  }
}