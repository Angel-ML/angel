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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The complex object that contains a integer array,, it can be stored in PS
 */
public class ByteArrayElement implements IElement {

  private byte[] data;

  public ByteArrayElement(byte[] data) {
    this.data = data;
  }

  public ByteArrayElement() {
    this(null);
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  @Override
  public ByteArrayElement deepClone() {
    byte[] newData = new byte[data.length];
    System.arraycopy(data, 0, newData, 0, data.length);
    return new ByteArrayElement(newData);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeBytes(output, data);
  }

  @Override
  public void deserialize(ByteBuf input) {
    data = ByteBufSerdeUtils.deserializeBytes(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedBytesLen(data);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeBytes(output, data);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    data = StreamSerdeUtils.deserializeBytes(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
