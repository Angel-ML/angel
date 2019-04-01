package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.collections.map.HashedMap;

public class LongElementMapStorage extends LongElementStorage {

  private Map<Long, IElement> data;

  public LongElementMapStorage(
      Class<? extends IElement> objectClass, int len, long indexOffset) {
    this(objectClass, new HashedMap(len), indexOffset);
  }

  public LongElementMapStorage(
      Class<? extends IElement> objectClass, Map<Long, IElement> data, long indexOffset) {
    super(objectClass, indexOffset);
    this.data = data;
  }

  public LongElementMapStorage() {
    this(null, 0, 0L);
  }

  @Override
  public IElement get(long index) {
    return data.get(index - indexOffset);
  }

  @Override
  public void set(long index, IElement value) {
    data.put(index - indexOffset, value);
  }

  @Override
  public IElement[] get(long[] indices) {
    IElement[] result = new IElement[indices.length];
    for (int i = 0; i < indices.length; i++) {
      result[i] = get(indices[i]);
    }
    return result;
  }

  @Override
  public void set(long[] indices, IElement[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], values[i]);
    }
  }

  @Override
  public boolean exist(long index) {
    return data.containsKey(index - indexOffset);
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public LongElementMapStorage deepClone() {
    Map<Long, IElement> clonedData = new HashedMap(data.size());
    for (Entry<Long, IElement> entry : data.entrySet()) {
      clonedData.put(entry.getKey(), (IElement) entry.getValue().deepClone());
    }
    return new LongElementMapStorage(objectClass, clonedData, indexOffset);
  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public boolean isDense() {
    return false;
  }

  @Override
  public boolean isSparse() {
    return true;
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    // Valid element number
    int writeIndex = buf.writerIndex();
    buf.writeInt(0);

    // Element data
    int writeNum = 0;
    for (Entry<Long, IElement> entry : data.entrySet()) {
      buf.writeLong(entry.getKey());
      entry.getValue().serialize(buf);
      writeNum++;
    }

    buf.setInt(writeIndex, writeNum);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    // Valid element number
    int elementNum = buf.readInt();
    data = new HashedMap(elementNum);

    // Deserialize the data
    for (int i = 0; i < elementNum; i++) {
      IElement element = newElement();
      data.put(buf.readLong(), element);
      element.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    int dataLen = 0;

    // Element data
    for (Entry<Long, IElement> entry : data.entrySet()) {
      dataLen += (8 + entry.getValue().bufferLen());
    }
    return super.bufferLen() + 4 + dataLen;
  }
}
