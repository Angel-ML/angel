/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.udf.getrow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

/**
 * The parameter of partition get row.
 */
public class PartitionGetRowParam implements Serialize {
  private int matrixId;
  private PartitionKey partKey;
  private int rowIndex;
  private int clock;
  private boolean bypassMode;

  /**
   * Creates a new partition parameter.
   *
   * @param matrixId   the matrix id
   * @param partKey    the part key
   * @param rowIndex   the row index
   * @param clock      the clock
   * @param bypassMode the bypass mode
   */
  public PartitionGetRowParam(int matrixId, PartitionKey partKey, int rowIndex, int clock,
      boolean bypassMode) {
    this.matrixId = matrixId;
    this.partKey = partKey;
    this.rowIndex = rowIndex;
    this.clock = clock;
    this.bypassMode = bypassMode;
  }

  /**
   * Creates a new partition parameter by default
   */
  public PartitionGetRowParam() {
    this(0, null, 0, -1, false);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(matrixId);
    buf.writeInt(rowIndex);
    buf.writeInt(clock);
    buf.writeBoolean(bypassMode);
    if (partKey != null) {
      partKey.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    matrixId = buf.readInt();
    rowIndex = buf.readInt();
    clock = buf.readInt();
    bypassMode = buf.readBoolean();

    if (buf.isReadable()) {
      if (partKey == null) {
        partKey = new PartitionKey();
      }

      partKey.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    return 16 + ((partKey != null) ? partKey.bufferLen() : 0);
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
   * Gets part key.
   *
   * @return the part key
   */
  public PartitionKey getPartKey() {
    return partKey;
  }

  /**
   * Sets matrix id.
   *
   * @param matrixId the matrix id
   */
  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  /**
   * Sets part key.
   *
   * @param partKey the part key
   */
  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }

  /**
   * Gets clock.
   *
   * @return the clock
   */
  public int getClock() {
    return clock;
  }

  /**
   * Is bypass mode boolean.
   *
   * @return the boolean
   */
  public boolean isBypassMode() {
    return bypassMode;
  }

  /**
   * Sets clock.
   *
   * @param clock the clock
   */
  public void setClock(int clock) {
    this.clock = clock;
  }

  /**
   * Sets bypass mode.
   *
   * @param bypassMode the bypass mode
   */
  public void setBypassMode(boolean bypassMode) {
    this.bypassMode = bypassMode;
  }

  /**
   * Gets row index.
   *
   * @return the row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  /**
   * Sets row index.
   *
   * @param rowIndex the row index
   */
  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }
}
