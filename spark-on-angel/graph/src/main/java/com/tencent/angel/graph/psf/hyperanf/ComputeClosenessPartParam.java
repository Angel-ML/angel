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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class ComputeClosenessPartParam extends PartitionUpdateParam {

  private int r;
  private boolean isConnected;

  public ComputeClosenessPartParam(int matrixId, PartitionKey pkey, int r, boolean isConnected) {
    super(matrixId, pkey);
    this.r = r;
    this.isConnected = isConnected;
  }

  public ComputeClosenessPartParam() {
    this(0, null, 0, true);
  }

  public int getR() {
    return r;
  }

  public boolean getIsConnected() {
    return isConnected;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(r);
    buf.writeBoolean(isConnected);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    r = buf.readInt();
    isConnected = buf.readBoolean();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    len += 1;
    return len;
  }
}