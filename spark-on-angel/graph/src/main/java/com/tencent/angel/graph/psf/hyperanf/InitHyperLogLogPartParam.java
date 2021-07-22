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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class InitHyperLogLogPartParam extends PartitionUpdateParam {

  private KeyPart nodes;
  private int p;
  private int sp;

  public InitHyperLogLogPartParam(int matrixId, PartitionKey partKey,
                                  KeyPart nodes, int p, int sp) {
    super(matrixId, partKey);
    this.nodes = nodes;
    this.p = p;
    this.sp = sp;
  }

  public InitHyperLogLogPartParam() {
  }

  public KeyPart getNodes() {
    return nodes;
  }

  public int getP() {
    return p;
  }

  public int getSp() {
    return sp;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeKeyPart(buf, nodes);
    ByteBufSerdeUtils.serializeInt(buf, p);
    ByteBufSerdeUtils.serializeInt(buf, sp);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    nodes = ByteBufSerdeUtils.deserializeKeyPart(buf);
    p = ByteBufSerdeUtils.deserializeInt(buf);
    sp = ByteBufSerdeUtils.deserializeInt(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedKeyPartLen(nodes)
            + ByteBufSerdeUtils.INT_LENGTH * 2;
  }
}
