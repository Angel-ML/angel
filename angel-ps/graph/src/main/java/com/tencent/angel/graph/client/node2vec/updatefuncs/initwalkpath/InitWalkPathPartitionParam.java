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
package com.tencent.angel.graph.client.node2vec.updatefuncs.initwalkpath;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitWalkPathPartitionParam extends PartitionUpdateParam {
  private int neighborMatrixId;
  private int walkLength;
  private int mod;

  public InitWalkPathPartitionParam(int matrixId, PartitionKey partKey, boolean updateClock, int neighborMatrixId, int walkLength, int mod) {
    super(matrixId, partKey, updateClock);
    this.neighborMatrixId = neighborMatrixId;
    this.walkLength = walkLength;
    this.mod = mod;
  }

  public InitWalkPathPartitionParam(int matrixId, PartitionKey partKey, int neighborMatrixId, int walkLength, int mod) {
    super(matrixId, partKey);
    this.neighborMatrixId = neighborMatrixId;
    this.walkLength = walkLength;
    this.mod = mod;
  }

  public InitWalkPathPartitionParam(int neighborMatrixId, int walkLength, int mod) {
    this.neighborMatrixId = neighborMatrixId;
    this.walkLength = walkLength;
    this.mod = mod;
  }

  public InitWalkPathPartitionParam() {

  }

  public int getNeighborMatrixId() {
    return neighborMatrixId;
  }

  public int getWalkLength() {
    return walkLength;
  }

  public int getMod() {
    return mod;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(neighborMatrixId);
    buf.writeInt(walkLength);
    buf.writeInt(mod);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    neighborMatrixId = buf.readInt();
    walkLength = buf.readInt();
    mod = buf.readInt();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    return len + 12;
  }
}
