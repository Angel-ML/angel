package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public class DynamicStringArray extends DynamicArray {

  private List<String[]> batchs;
  private String[] currentBatch;
  private String[] data;

  public DynamicStringArray(int size) {
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }
    batchs.add(new String[batchSize]);
    currentBatch = batchs.get(currentBatchIndex);
  }

  public DynamicStringArray() {
    this(0);
  }

  public void add(String value) {
    currentBatch[currentBatchPos] = value;
    currentBatchPos++;

    if (currentBatchPos == batchSize) {
      currentBatchPos = 0;
      currentBatchIndex++;
      if (currentBatchIndex < batchs.size()) {
        currentBatch = batchs.get(currentBatchIndex);
      } else {
        batchs.add(new String[batchSize]);
        currentBatch = batchs.get(currentBatchIndex);
      }
    }
  }

  public String[] getData() {
    if (data == null) {
      int index = 0;
      data = new String[size()];
      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex; i++) {
        String[] batch = batchs.get(i);
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
    ByteBufSerdeUtils.serializeInt(out, size());

    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      String[] batch = batchs.get(i);
      for (int j = 0; j < batch.length; j++) {
        ByteBufSerdeUtils.serializeUTF8(out, batch[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      ByteBufSerdeUtils.serializeUTF8(out, currentBatch[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    data = ByteBufSerdeUtils.deserializeUTF8s(in);
  }

  @Override
  public int bufferLen() {
    int len = ByteBufSerdeUtils.serializedIntLen(size());
    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      String[] batch = batchs.get(i);
      for (int j = 0; j < batch.length; j++) {
        len += ByteBufSerdeUtils.serializedUTF8Len(batch[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      len += ByteBufSerdeUtils.serializedUTF8Len(currentBatch[i]);
    }
    return len;
  }
}
