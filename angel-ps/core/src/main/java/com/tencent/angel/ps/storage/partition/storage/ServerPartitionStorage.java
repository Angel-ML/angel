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

package com.tencent.angel.ps.storage.partition.storage;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Base class of server partition storage
 */
public abstract class ServerPartitionStorage implements IServerPartitionStorage {
  /**
   * Row id offset
   */
  protected volatile int rowIdOffset;

  public ServerPartitionStorage(int rowIdOffset) {
    this.rowIdOffset = rowIdOffset;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(rowIdOffset);
  }

  @Override
  public void deserialize(ByteBuf input) {
    rowIdOffset = input.readInt();
  }

  @Override
  public int bufferLen() {
    return 4;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(rowIdOffset);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    rowIdOffset = input.readInt();
  }

  @Override
  public int dataLen() {
    return 4;
  }

  @Override
  public void update(UpdateFunc func, PartitionUpdateParam partParam) {
    func.partitionUpdate(partParam);
  }
}
