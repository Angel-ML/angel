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
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;

public class ElementElementMapStorage extends ElementElementStorage {

  private Object2ObjectOpenHashMap<IElement, IElement> data;

  public ElementElementMapStorage(
      Class<? extends IElement> objectClass, int len, long indexOffset) {
    this(objectClass, new Object2ObjectOpenHashMap<>(len), indexOffset);
  }

  public ElementElementMapStorage(
      Class<? extends IElement> objectClass, Object2ObjectOpenHashMap<IElement, IElement> data,
      long indexOffset) {
    super(objectClass, indexOffset);
    this.data = data;
  }

  public ElementElementMapStorage() {
    this(null, 0, 0L);
  }

  @Override
  public IElement get(IElement key) {
    return data.get(key);
  }

  @Override
  public void set(IElement key, IElement value) {
    data.put(key, value);
  }

  @Override
  public ObjectIterator<Object2ObjectMap.Entry<IElement, IElement>> iterator() {
    return this.data.object2ObjectEntrySet().fastIterator();
  }

  @Override
  public IElement[] getKeys() {
    return this.data.keySet().toArray(new IElement[data.size()]);
  }

  @Override
  public IElement[] get(IElement[] keys) {
    IElement[] result = new IElement[keys.length];
    for (int i = 0; i < keys.length; i++) {
      result[i] = get(keys[i]);
    }
    return result;
  }

  @Override
  public void set(IElement[] keys, IElement[] values) {
    assert keys.length == values.length;
    for (int i = 0; i < keys.length; i++) {
      set(keys[i], values[i]);
    }
  }

  @Override
  public boolean exist(IElement key) {
    return data.containsKey(key);
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public ElementElementMapStorage deepClone() {
    Object2ObjectOpenHashMap<IElement, IElement> clonedData = new Object2ObjectOpenHashMap(
        data.size());
    ObjectIterator<Object2ObjectMap.Entry<IElement, IElement>> iter = clonedData
        .object2ObjectEntrySet().fastIterator();
    while (iter.hasNext()) {
      Object2ObjectMap.Entry<IElement, IElement> entry = iter.next();
      clonedData.put(entry.getKey(), (IElement) entry.getValue().deepClone());
    }
    return new ElementElementMapStorage(objectClass, clonedData, indexOffset);
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
  public ElementElementMapStorage adaptiveClone() {
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
    for (Entry<IElement, IElement> entry : data.entrySet()) {
      entry.getKey().serialize(buf);
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
    data = new Object2ObjectOpenHashMap<>(elementNum);

    // Deserialize the data
    for (int i = 0; i < elementNum; i++) {
      IElement elementKey = newElement();
      IElement elementValue = newElement();
      data.put(elementKey, elementValue);
      elementKey.deserialize(buf);
      elementValue.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    int dataLen = 0;

    // Element data
    for (Entry<IElement, IElement> entry : data.entrySet()) {
      dataLen += (entry.getKey().bufferLen() + entry.getValue().bufferLen());
    }
    return super.bufferLen() + 4 + dataLen;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    super.serialize(output);
    // Valid element number
    output.writeInt(data.size());

    // Element data
    for (Entry<IElement, IElement> entry : data.entrySet()) {
      entry.getKey().serialize(output);
      entry.getValue().serialize(output);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    super.deserialize(input);

    // Valid element number
    int elementNum = input.readInt();
    data = new Object2ObjectOpenHashMap<>(elementNum);

    // Deserialize the data
    for (int i = 0; i < elementNum; i++) {
      IElement elementKey = newElement();
      IElement elementValue = newElement();
      data.put(elementKey, elementValue);
      elementKey.deserialize(input);
      elementValue.deserialize(input);
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public Object2ObjectOpenHashMap<IElement, IElement> getData() {
    return data;
  }

  public void setData(
      Object2ObjectOpenHashMap<IElement, IElement> data) {
    this.data = data;
  }
}