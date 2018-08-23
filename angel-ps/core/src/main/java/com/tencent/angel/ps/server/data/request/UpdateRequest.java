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


package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;

public class UpdateRequest extends PartitionRequest {
  /**
   * task index, used for consistency control
   */
  private int taskIndex;

  /**
   * the update row splits
   */
  private UpdateItem rowsSplit;

  /**
   * weather we need update the clock of the matrix partition
   */
  private boolean updateClock;

  /**
   * Update method
   */
  private UpdateOp op;

  /**
   * Create PutPartitionUpdateRequest.
   *
   * @param userRequestId user request id
   * @param taskIndex     task index
   * @param clock         clock value
   * @param partKey       matrix partition key
   * @param rowsSplit     update row splits
   * @param updateClock   true means update the clock value of the matrix partition
   */
  public UpdateRequest(int userRequestId, int taskIndex, int clock, PartitionKey partKey,
    UpdateItem rowsSplit, boolean updateClock, UpdateOp op) {
    super(userRequestId, clock, partKey);
    this.setTaskIndex(taskIndex);
    this.setRowsSplit(rowsSplit);
    this.setUpdateClock(updateClock);
    this.setOp(op);
  }

  public UpdateRequest() {
    this(-1, 0, 0, null, null, false, UpdateOp.PLUS);
  }

  @Override public int getEstimizeDataSize() {
    return bufferLen();
  }

  @Override public TransportMethod getType() {
    return TransportMethod.UPDATE;
  }

  /**
   * Is we need update the clock of the matrix partition.
   *
   * @return boolean true means update the clock
   */
  public boolean isUpdateClock() {
    return updateClock;
  }

  /**
   * Set is we need update the clock of the matrix partition.
   *
   * @param updateClock true means update the clock
   */
  public void setUpdateClock(boolean updateClock) {
    this.updateClock = updateClock;
  }

  /**
   * Get update row splits.
   *
   * @return List<RowUpdateSplit> update row splits.
   */
  public UpdateItem getRowsSplit() {
    return rowsSplit;
  }

  /**
   * Set update row splits.
   *
   * @param rowsSplit update row splits.
   */
  public void setRowsSplit(UpdateItem rowsSplit) {
    this.rowsSplit = rowsSplit;
  }

  public UpdateOp getOp() {
    return op;
  }

  public void setOp(UpdateOp op) {
    this.op = op;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(taskIndex);
    buf.writeBoolean(updateClock);
    buf.writeInt(op.getOpId());
    if (rowsSplit != null) {
      rowsSplit.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    taskIndex = buf.readInt();
    updateClock = buf.readBoolean();
    op = UpdateOp.valueOf(buf.readInt());
    rowsSplit = null;
  }

  @Override public int bufferLen() {
    int len = super.bufferLen() + 12;
    if (rowsSplit != null) {
      len += rowsSplit.bufferLen();
    }
    return len;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public void setTaskIndex(int taskIndex) {
    this.taskIndex = taskIndex;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((rowsSplit == null) ? 0 : rowsSplit.hashCode());
    result = prime * result + taskIndex;
    result = prime * result + (updateClock ? 1231 : 1237);
    return result;
  }

  @Override public int getHandleElemNum() {
    if (rowsSplit != null) {
      handleElemSize = rowsSplit.size();
    }
    return handleElemSize;
  }

  @Override public boolean equals(Object obj) {
    return false;
  }

  @Override public String toString() {
    return "PutPartitionUpdateRequest [taskIndex=" + taskIndex + ", rowsSplit size=" + ((rowsSplit
      != null) ? rowsSplit.size() : 0) + ", updateClock=" + updateClock + ", toString()=" + super
      .toString() + "]";
  }
}
