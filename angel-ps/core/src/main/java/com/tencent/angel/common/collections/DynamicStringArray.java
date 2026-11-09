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

  // Wire format must match ByteBufSerdeUtils.serializeUTF8s/deserializeUTF8s:
  // [int size] then for each element [boolean notNull][utf8 bytes if notNull].
  private static void serializeElement(ByteBuf out, String value) {
    if (value != null) {
      ByteBufSerdeUtils.serializeBoolean(out, true);
      ByteBufSerdeUtils.serializeUTF8(out, value);
    } else {
      ByteBufSerdeUtils.serializeBoolean(out, false);
    }
  }

  private static int elementLen(String value) {
    int len = ByteBufSerdeUtils.BOOLEN_LENGTH;
    if (value != null) {
      len += ByteBufSerdeUtils.serializedUTF8Len(value);
    }
    return len;
  }

  @Override
  public void serialize(ByteBuf out) {
    ByteBufSerdeUtils.serializeInt(out, size());

    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      String[] batch = batchs.get(i);
      for (int j = 0; j < batch.length; j++) {
        serializeElement(out, batch[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      serializeElement(out, currentBatch[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    data = ByteBufSerdeUtils.deserializeUTF8s(in);
    deserializedSize = data.length;
  }

  @Override
  public int bufferLen() {
    int len = ByteBufSerdeUtils.serializedIntLen(size());
    // [0, currentBatchIndex - 1] batchs
    for (int i = 0; i < currentBatchIndex; i++) {
      String[] batch = batchs.get(i);
      for (int j = 0; j < batch.length; j++) {
        len += elementLen(batch[j]);
      }
    }

    // Last batch
    for (int i = 0; i < currentBatchPos; i++) {
      len += elementLen(currentBatch[i]);
    }
    return len;
  }
}
