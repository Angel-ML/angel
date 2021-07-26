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
package com.tencent.angel.graph.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class GetHyperLogLogPartParam extends PartitionGetParam {

  private KeyPart nodes;
  private long n;
  private boolean isDirected;

  public GetHyperLogLogPartParam(int matrixId, PartitionKey partKey, KeyPart nodes, long n,
                                 boolean isDirected) {
    super(matrixId, partKey);
    this.nodes = nodes;
    this.n = n;
    this.isDirected = isDirected;
  }

  public GetHyperLogLogPartParam() {
  }

  public KeyPart getNodes() {
    return nodes;
  }

  public void setNodes(KeyPart nodes) {
    this.nodes = nodes;
  }

  public long getN() {
    return n;
  }

  public void setN(long n) {
    this.n = n;
  }

  public boolean isDirected() {
    return isDirected;
  }

  public void setDirected(boolean directed) {
    isDirected = directed;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeKeyPart(buf, nodes);
    ByteBufSerdeUtils.serializeLong(buf, n);
    ByteBufSerdeUtils.serializeBoolean(buf, isDirected);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    nodes = ByteBufSerdeUtils.deserializeKeyPart(buf);
    n = ByteBufSerdeUtils.deserializeLong(buf);
    isDirected = ByteBufSerdeUtils.deserializeBoolean(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedKeyPartLen(nodes)
            + ByteBufSerdeUtils.LONG_LENGTH + ByteBufSerdeUtils.BOOLEN_LENGTH;
  }
}
