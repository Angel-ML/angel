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


package com.tencent.angel.ml.matrix.psf.get.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.server.data.request.ValueType;
import io.netty.buffer.ByteBuf;

/**
 * Base class for partition index get row result
 */
public abstract class IndexPartGetResult extends PartitionGetResult {
  private PartitionKey partKey;

  public IndexPartGetResult(PartitionKey partKey) {
    this.partKey = partKey;
  }

  public IndexPartGetResult() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {
    partKey.serialize(buf);
  }

  @Override public void deserialize(ByteBuf buf) {
    partKey = new PartitionKey();
    partKey.deserialize(buf);
  }

  @Override public int bufferLen() {
    return (partKey != null) ? partKey.bufferLen() : 0;
  }

  public PartitionKey getPartKey() {
    return partKey;
  }

  public abstract ValueType getValueType();
}
