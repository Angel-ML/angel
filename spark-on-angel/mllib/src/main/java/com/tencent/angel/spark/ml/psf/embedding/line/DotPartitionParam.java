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

package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class DotPartitionParam extends PartitionGetParam {

  int seed;
  int partitionId;
  private byte[] bufData;
  ByteBuf edgeBuf;
  private int bufLength;

  public DotPartitionParam(int matrixId,
                           PartitionKey pkey,
                           byte[] bufData,
                           int bufLength) {
    super(matrixId, pkey);
    this.bufData = bufData;
    this.bufLength = bufLength;
  }

  public DotPartitionParam() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBytes(bufData);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.seed = buf.readInt();
    this.partitionId = buf.readInt();
    this.edgeBuf = buf;
    buf.retain();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + this.bufLength;
  }

  public void clear() {
    edgeBuf.release();
  }
}
