package com.tencent.angel.common.collections;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public class DynamicObjectArray extends DynamicArray {

  private List<IElement[]> batchs;
  private IElement[] currentBatch;
  private IElement[] data;
  private String keyClass;

  public DynamicObjectArray(int size) {
    if (size > 0) {
      batchs = new ArrayList<>(size / batchSize + 1);
    } else {
      batchs = new ArrayList<>(defaultBatchNum);
    }

    batchs.add(new IElement[batchSize]);
    currentBatch = batchs.get(0);
  }

  public DynamicObjectArray() {
  }

  public void add(IElement value) {
    if (value != null && keyClass == null) {
      keyClass = value.getClass().getName();
    }

    currentBatch[currentBatchPos] = value;
    currentBatchPos++;

    if (currentBatchPos == batchSize) {
      currentBatchPos = 0;
      currentBatchIndex++;
      if (currentBatchIndex < batchs.size()) {
        currentBatch = batchs.get(currentBatchIndex);
      } else {
        batchs.add(new IElement[batchSize]);
        currentBatch = batchs.get(currentBatchIndex);
      }
    }
  }

  public IElement[] getData() {
    if (data == null) {
      int index = 0;
      data = new IElement[size()];
      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex - 1; i++) {
        IElement[] batch = batchs.get(i);
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
    int elemNum = size();

    // Key size
    ByteBufSerdeUtils.serializeInt(out, elemNum);

    // Key class name
    if (elemNum > 0 && keyClass != null) {
      ByteBufSerdeUtils.serializeUTF8(out, keyClass);
      // Keys
      // [0, currentBatchIndex - 1] batchs
      for (int i = 0; i < currentBatchIndex; i++) {
        IElement[] batch = batchs.get(i);
        for (int j = 0; j < batch.length; j++) {
          if (batch[j] != null) {
            ByteBufSerdeUtils.serializeBoolean(out, true);
            batch[j].serialize(out);
          } else {
            ByteBufSerdeUtils.serializeBoolean(out, false);
          }
        }
      }

      // Last batch
      for (int i = 0; i < currentBatchPos; i++) {
        if (currentBatch[i] != null) {
          ByteBufSerdeUtils.serializeBoolean(out, true);
          currentBatch[i].serialize(out);
        } else {
          ByteBufSerdeUtils.serializeBoolean(out, false);
        }
      }
    } else if (elemNum > 0) {
      ByteBufSerdeUtils.serializeUTF8(out, defaultValueClass);
      for (int i = 0; i < elemNum; i++) {
        ByteBufSerdeUtils.serializeBoolean(out, false);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf in) {
    data = ByteBufSerdeUtils.deserializeObjects(in);
  }

  @Override
  public int bufferLen() {
    int elemNum = size();

    int len = 0;
    // Values
    len += ByteBufSerdeUtils.INT_LENGTH;
    if (elemNum > 0 && keyClass != null) {
      len += ByteBufSerdeUtils.serializedUTF8Len(keyClass);
      for (int i = 0; i < currentBatchIndex; i++) {
        IElement[] batch = batchs.get(i);
        for (int j = 0; j < batch.length; j++) {
          len += ByteBufSerdeUtils.BOOLEN_LENGTH;
          if (batch[j] != null) {
            len += batch[j].bufferLen();
          }
        }
      }

      // Last batch
      for (int i = 0; i < currentBatchPos; i++) {
        len += ByteBufSerdeUtils.BOOLEN_LENGTH;
        if (currentBatch[i] != null) {
          len += currentBatch[i].bufferLen();
        }
      }
    } else if (elemNum > 0) {
      len += ByteBufSerdeUtils.serializedUTF8Len(defaultValueClass);
      len += elemNum * ByteBufSerdeUtils.BOOLEN_LENGTH;
    }

    return len;
  }
}
