package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public class DynamicDoubleArray extends DynamicArray {
  private List<double[]> batchs;
  private double[] currentBatch;
  private double[] data;

  public DynamicDoubleArray(int size) {
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }

    batchs.add(new double[batchSize]);
    currentBatch = batchs.get(0);
  }

  public DynamicDoubleArray() {
  }

  public void add(double value) {
    currentBatch[currentBatchPos] = value;
    currentBatchPos++;

    if (currentBatchPos == batchSize) {
      currentBatchPos = 0;
      currentBatchIndex++;
      if (currentBatchIndex < batchs.size()) {
        currentBatch = batchs.get(currentBatchIndex);
      } else {
        batchs.add(new double[batchSize]);
        currentBatch = batchs.get(currentBatchIndex);
      }
    }
  }

  public List<double[]> getBatchs() {
    return batchs;
  }

  public double[] getCurrentBatch() {
    return currentBatch;
  }

  public double[] getData() {
    if (data == null) {
      data = new double[size()];
      // [0, currentBatchIndex - 1] batchs
      int offset = 0;
      for (int i = 0; i < currentBatchIndex; i++) {
        double[] batch = batchs.get(i);
        System.arraycopy(batch, 0, data, offset, batch.length);
        offset += batch.length;
      }

      // Last batch
      for (int i = 0; i < currentBatchPos; i++) {
        System.arraycopy(currentBatch, 0, data, offset, currentBatchPos);
      }
    }
    return data;
  }


  @Override
  public void serialize(ByteBuf out) {
    ByteBufSerdeUtils.serializeInt(out, size());

    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      double[] batch = batchs.get(i);
      for (int j = 0; j < batch.length; j++) {
        ByteBufSerdeUtils.serializeDouble(out, batch[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      ByteBufSerdeUtils.serializeDouble(out, currentBatch[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    data = ByteBufSerdeUtils.deserializeDoubles(in);
  }

  @Override
  public int bufferLen() {
    int elemNum = size();
    return ByteBufSerdeUtils.INT_LENGTH + elemNum * ByteBufSerdeUtils.DOUBLE_LENGTH;
  }
}
