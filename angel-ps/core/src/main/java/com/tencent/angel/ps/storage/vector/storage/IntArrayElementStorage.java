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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Complex object storage which use a array as storage structure
 */
public class IntArrayElementStorage extends IntElementStorage {
  /**
   * Complex object storage
   */
  private IElement[] data;

  public IntArrayElementStorage(Class<? extends IElement> objectClass, int len, long indexOffset) {
    this(objectClass, new IElement[len], indexOffset);
  }

  public IntArrayElementStorage(Class<? extends IElement> objectClass, IElement[] data,
      long indexOffset) {
    super(objectClass, indexOffset);
    this.data = data;
  }

  public IntArrayElementStorage() {
    this(null, 0, 0L);
  }

  public IElement[] getData() {
    return data;
  }

  public void setData(IElement[] data) {
    this.data = data;
  }

  @Override
  public boolean exist(int index) {
    return data[index - (int) indexOffset] != null;
  }

  @Override
  public IElement get(int index) {
    return data[index - (int) indexOffset];
  }

  @Override
  public void set(int index, IElement value) {
    data[index - (int) indexOffset] = value;
  }

  @Override
  public IElement[] get(int[] indices) {
    IElement[] result = new IElement[indices.length];
    for (int i = 0; i < indices.length; i++) {
      result[i] = get(indices[i]);
    }
    return result;
  }

  @Override
  public void set(int[] indices, IElement[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], values[i]);
    }
  }

  @Override
  public void clear() {
    for (int i = 0; i < data.length; i++) {
      data[i] = null;
    }
  }

  @Override
  public IntArrayElementStorage deepClone() {
    IElement[] cloneData = new IElement[data.length];
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        cloneData[i] = (IElement) data[i].deepClone();
      }
    }
    return new IntArrayElementStorage(objectClass, cloneData, indexOffset);
  }

  @Override
  public int size() {
    return data.length;
  }

  @Override
  public boolean isDense() {
    return true;
  }

  @Override
  public boolean isSparse() {
    return false;
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public IntArrayElementStorage adaptiveClone() {
    return this;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    // Use sparse storage method, as some elements in the array maybe null
    // Array length
    buf.writeInt(data.length);

    // Valid element number
    int writeIndex = buf.writerIndex();
    buf.writeInt(0);

    // Element data
    int writeNum = 0;
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        buf.writeInt(i);
        data[i].serialize(buf);
        writeNum++;
      }
    }
    buf.setInt(writeIndex, writeNum);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    // Array len
    int len = buf.readInt();
    data = new IElement[len];

    // Valid element number
    int elementNum = buf.readInt();

    for (int i = 0; i < elementNum; i++) {
      IElement element = newElement();
      data[buf.readInt()] = element;
      element.deserialize(buf);
    }
  }


  @Override
  public int bufferLen() {
    int dataLen = 0;
    for (int i = 0; i < data.length; i++) {
      dataLen += (4 + data[i].bufferLen());
    }
    return super.bufferLen() + 8 + dataLen;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    super.serialize(output);
    // Use sparse storage method, as some elements in the array maybe null
    // Array length
    output.writeInt(data.length);

    // Valid element number
    // Element data
    int writeNum = 0;
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        writeNum++;
      }
    }
    output.writeInt(writeNum);

    // Element data
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        output.writeInt(i);
        data[i].serialize(output);
      }
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    super.deserialize(input);
    // Array len
    int len = input.readInt();
    data = new IElement[len];

    // Valid element number
    int elementNum = input.readInt();

    for (int i = 0; i < elementNum; i++) {
      IElement element = newElement();
      data[input.readInt()] = element;
      element.deserialize(input);
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
