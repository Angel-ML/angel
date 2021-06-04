/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;

public class LongElementMapStorage extends LongElementStorage {

  private Long2ObjectOpenHashMap<IElement> data;
  private volatile long[] keys;

  public LongElementMapStorage(
      Class<? extends IElement> objectClass, int len, long indexOffset) {
    this(objectClass, new Long2ObjectOpenHashMap(len), indexOffset);
  }

  public LongElementMapStorage(
      Class<? extends IElement> objectClass, Long2ObjectOpenHashMap<IElement> data, long indexOffset) {
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
  public ObjectIterator<Long2ObjectMap.Entry<IElement>> iterator() {
    return this.data.long2ObjectEntrySet().fastIterator();
  }

  @Override
  public IElement[] get(long[] indices) {
    IElement[] result = new IElement[indices.length];
    for (int i = 0; i < indices.length; i++) {
      result[i] = get(indices[i]);
    }
    return result;
  }

  public long getKey(int index) {
    if (keys == null) {
      synchronized (LongElementMapStorage.class) {
        if (keys == null) {
          keys = data.keySet().toLongArray();
        }
      }
    }
    return keys[index];
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
    Long2ObjectOpenHashMap<IElement> clonedData = new Long2ObjectOpenHashMap(data.size());
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = clonedData
        .long2ObjectEntrySet().fastIterator();
    while(iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      clonedData.put(entry.getLongKey(), (IElement) entry.getValue().deepClone());
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
  public LongElementMapStorage adaptiveClone() {
    return this;
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
    data = new Long2ObjectOpenHashMap<>(elementNum);

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

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    super.serialize(output);
    // Valid element number
    output.writeInt(data.size());

    // Element data
    int writeNum = 0;
    for (Entry<Long, IElement> entry : data.entrySet()) {
      output.writeLong(entry.getKey());
      entry.getValue().serialize(output);
      writeNum++;
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    super.deserialize(input);

    // Valid element number
    int elementNum = input.readInt();
    data = new Long2ObjectOpenHashMap<>(elementNum);

    // Deserialize the data
    for (int i = 0; i < elementNum; i++) {
      IElement element = newElement();
      data.put(input.readLong(), element);
      element.deserialize(input);
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
