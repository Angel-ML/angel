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
import io.netty.buffer.ByteBuf;

/**
 * Base class for partition index get row result
 */
public abstract class IndexPartGetRowResult implements Serialize {
  private volatile PartitionKey partKey;
  private volatile IndicesView indices;

  public IndexPartGetRowResult(PartitionKey partKey, IndicesView indices) {
    this.partKey = partKey;
    this.indices = indices;
  }

  public PartitionKey getPartKey() {
    return partKey;
  }

  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }

  public IndicesView getIndices() {
    return indices;
  }

  public void setIndices(IndicesView indices) {
    this.indices = indices;
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
}