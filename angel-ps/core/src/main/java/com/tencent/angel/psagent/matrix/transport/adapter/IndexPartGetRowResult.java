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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

/**
 * Base class for partition index get row result
 */
public abstract class IndexPartGetRowResult implements Serialize {
  private transient volatile PartitionKey partKey;
  private transient volatile KeyPart keyPart;

  public IndexPartGetRowResult(PartitionKey partKey, KeyPart keyPart) {
    this.partKey = partKey;
    this.keyPart = keyPart;
  }

  public IndexPartGetRowResult() {
    this(null, null);
  }

  public PartitionKey getPartKey() {
    return partKey;
  }

  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }

  @Override public void serialize(ByteBuf buf) {
    serializeData(buf);
  }

  @Override public void deserialize(ByteBuf buf) {
    deserializeData(buf);
  }

  @Override public int bufferLen() {
    return getDataSize();
  }

  public abstract void serializeData(ByteBuf buf);

  public abstract void deserializeData(ByteBuf buf);

  public abstract int getDataSize();

  public abstract ValueType getValueType();

  public KeyPart getKeyPart() {
    return keyPart;
  }

  public void setKeyPart(KeyPart keyPart) {
    this.keyPart = keyPart;
  }
}