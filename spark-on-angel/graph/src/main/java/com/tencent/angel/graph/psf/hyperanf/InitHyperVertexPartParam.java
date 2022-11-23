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
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class InitHyperVertexPartParam extends PartitionUpdateParam {

  private KeyValuePart keyValuePart;
  private int p;
  private int sp;
  private long seed;

  public InitHyperVertexPartParam(int matrixId, PartitionKey partKey, KeyValuePart keyValuePart, int p, int sp, long seed) {
    super(matrixId, partKey);
    this.keyValuePart = keyValuePart;
    this.p = p;
    this.sp = sp;
    this.seed = seed;
  }

  public InitHyperVertexPartParam() {

  }

  public KeyValuePart getKeyValuePart() {
    return keyValuePart;
  }

  public int getP() {
    return p;
  }

  public int getSp() { return sp; }

  public long getSeed() { return seed; }


  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, p);
    ByteBufSerdeUtils.serializeInt(buf, sp);
    ByteBufSerdeUtils.serializeLong(buf, seed);
    ByteBufSerdeUtils.serializeKeyValuePart(buf, keyValuePart);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    p = ByteBufSerdeUtils.deserializeInt(buf);
    sp = ByteBufSerdeUtils.deserializeInt(buf);
    seed = ByteBufSerdeUtils.deserializeLong(buf);
    keyValuePart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedIntLen(p)
        + ByteBufSerdeUtils.serializedIntLen(sp)
        + ByteBufSerdeUtils.serializedLongLen(seed)
        + ByteBufSerdeUtils.serializedKeyValuePartLen(keyValuePart);
  }
}