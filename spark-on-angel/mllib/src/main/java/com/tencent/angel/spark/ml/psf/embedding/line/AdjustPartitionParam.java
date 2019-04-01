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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class AdjustPartitionParam extends PartitionUpdateParam {

  int seed;
  int partitionId;
  private byte[] dataBuf;
  ByteBuf edgesAndGradsBuf;
  private int bufLength;

  public AdjustPartitionParam(int matrixId,
                              PartitionKey partKey,
                              byte[] dataBuf,
                              int bufLength) {
    super(matrixId, partKey);
    this.dataBuf = dataBuf;
    this.bufLength = bufLength;
  }

  public AdjustPartitionParam() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBytes(dataBuf);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    seed = buf.readInt();
    partitionId = buf.readInt();
    edgesAndGradsBuf = buf;
    edgesAndGradsBuf.retain();
  }

  public void clear() {
    edgesAndGradsBuf.release();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + this.bufLength;
  }
}
