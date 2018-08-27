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


package com.tencent.angel.ml.matrix.psf.update.base;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

/**
 * The skeleton of partition updater parameter.
 */
public abstract class PartitionUpdateParam implements Serialize {
  private int matrixId;
  private PartitionKey partKey;
  private boolean updateClock;

  /**
   * Creates a new partition parameter.
   *
   * @param matrixId    the matrix id
   * @param partKey     the part key
   * @param updateClock the update clock
   */
  public PartitionUpdateParam(int matrixId, PartitionKey partKey, boolean updateClock) {
    this.matrixId = matrixId;
    this.partKey = partKey;
    this.updateClock = updateClock;
  }

  /**
   * Creates a new partition parameter.
   *
   * @param matrixId the matrix id
   * @param partKey  the part key
   */
  public PartitionUpdateParam(int matrixId, PartitionKey partKey) {
    this(matrixId, partKey, false);
  }

  /**
   * Creates a new partition parameter by default.
   */
  public PartitionUpdateParam() {
    this(0, null, false);
  }

  /**
   * Is update clock.
   *
   * @return the result
   */
  public boolean isUpdateClock() {
    return updateClock;
  }

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Gets partition key.
   *
   * @return the partition key
   */
  public PartitionKey getPartKey() {
    return partKey;
  }

  @Override public void serialize(ByteBuf buf) {
    buf.writeInt(matrixId);
    buf.writeBoolean(updateClock);
    if (partKey != null) {
      partKey.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    matrixId = buf.readInt();
    updateClock = buf.readBoolean();

    if (buf.isReadable()) {
      if (partKey == null) {
        partKey = new PartitionKey();
      }
      partKey.deserialize(buf);
    }
  }

  @Override public int bufferLen() {
    return 8 + ((partKey != null) ? partKey.bufferLen() : 0);
  }
}
