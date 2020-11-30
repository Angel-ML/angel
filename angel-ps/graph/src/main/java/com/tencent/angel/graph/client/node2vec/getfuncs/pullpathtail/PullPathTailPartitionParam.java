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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PullPathTailPartitionParam extends PartitionGetParam {
  private int partitionId;
  private int batchSize;

  public PullPathTailPartitionParam(int matrixId, PartitionKey partKey, int partitionId, int batchSize) {
    super(matrixId, partKey);
    this.partitionId = partitionId;
    this.batchSize = batchSize;
  }

  public PullPathTailPartitionParam(int partitionId, int batchSize) {
    this.partitionId = partitionId;
    this.batchSize = batchSize;
  }

  public PullPathTailPartitionParam() {
    super();
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(partitionId);
    buf.writeInt(batchSize);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    partitionId = buf.readInt();
    batchSize = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return 8 + super.bufferLen();
  }

}
