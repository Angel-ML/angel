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

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

/**
 * Partition Sample parameter of SampleNeighbor
 */
public class PartSampleNeighborParam extends GeneralPartGetParam {

  /**
   * Sample number, if count <= 0, means return all neighbors
   */
  private int count;

  /**
   * Sample method, such as: random, aliastable, simplereject
   */
  private SampleMethod method;

  /**
   * Neighbor storage type, such as: INTARRAY, LONGARRAY
   */
  private NeighborStorageType storageType;

  public PartSampleNeighborParam(int matrixId, PartitionKey part, KeyPart indicesPart, int count,
      SampleMethod method, NeighborStorageType storageType) {
    super(matrixId, part, indicesPart);
    this.count = count;
    this.method = method;
    this.storageType = storageType;
  }

  public PartSampleNeighborParam() {
    this(0, null, null, 0, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public SampleMethod getMethod() {
    return method;
  }

  public void setMethod(SampleMethod method) {
    this.method = method;
  }

  public NeighborStorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(NeighborStorageType storageType) {
    this.storageType = storageType;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, count);
    ByteBufSerdeUtils.serializeInt(buf, method.getIndex());
    if (storageType != null) {
      ByteBufSerdeUtils.serializeByte(buf, storageType.getIndex());
    } else {
      ByteBufSerdeUtils.serializeByte(buf, NeighborStorageType.LONGARRAY.getIndex());
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = ByteBufSerdeUtils.deserializeInt(buf);
    method = SampleMethod.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
    storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(buf));
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH * 2 + ByteBufSerdeUtils.BYTE_LENGTH;
  }
}