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


package com.tencent.angel.graph.model.neighbor.dynamic;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.common.collections.DynamicLongArray;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.twitter.chill.ScalaKryoInstantiator;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * The complex object that contains a integer array,, it can be stored in PS
 */
public class DynamicNeighborElement implements IElement {

  private byte[] data;
  private DynamicLongArray dynamicData;

  public DynamicNeighborElement(byte[] data) {
    this.data = data;
    this.dynamicData = null;
  }

  public DynamicNeighborElement() {
    this.dynamicData = new DynamicLongArray(-1);
    this.data = new byte[0];
  }

  public byte[] getData() {
    return data;
  }

  public void trans() {
    long[] temp = dynamicData.getData();
    Arrays.sort(temp);
    this.data = ScalaKryoInstantiator.defaultPool().toBytesWithoutClass(temp);
    this.dynamicData = null;
  }

  public void add(long[] in) {
    for (int i = 0; i < in.length; i ++) {
      this.dynamicData.add(in[i]);
    }
  }

  @Override
  public DynamicNeighborElement deepClone() {
    byte[] newData = new byte[data.length];
    System.arraycopy(data, 0, newData, 0, data.length);
    return new DynamicNeighborElement(newData);
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
    return 4 + data.length;
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
