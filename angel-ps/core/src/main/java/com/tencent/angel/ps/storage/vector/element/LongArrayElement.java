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


package com.tencent.angel.ps.storage.vector.element;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang.ArrayUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * The complex object that contains a integer array,, it can be stored in PS
 */
public class LongArrayElement implements IElement {

  private long[] data;

  public LongArrayElement(long[] data) {
    this.data = data;
  }

  public LongArrayElement() {
    this(null);
  }

  public long[] getData() {
    return data;
  }

  public void setData(long[] data) {
    this.data = data;
  }

  public void add(long[] incs) {
    this.data = ArrayUtils.addAll(data, incs);
    Arrays.sort(this.data);
  }

  @Override
  public LongArrayElement deepClone() {
    long[] newData = new long[data.length];
    System.arraycopy(data, 0, newData, 0, data.length);
    return new LongArrayElement(newData);
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(data.length);
    for (int i = 0; i < data.length; i++) {
      output.writeLong(data[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    data = new long[input.readInt()];
    for (int i = 0; i < data.length; i++) {
      data[i] = input.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return 4 + data.length * 8;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(data.length);
    for (int i = 0; i < data.length; i++) {
      output.writeLong(data[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    data = new long[input.readInt()];
    for (int i = 0; i < data.length; i++) {
      data[i] = input.readLong();
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
