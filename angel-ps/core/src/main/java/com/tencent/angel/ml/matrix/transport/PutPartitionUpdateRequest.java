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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Update matrix partition rpc request.
 */
public class PutPartitionUpdateRequest extends PartitionRequest {
  /** task index, used for consistency control */
  private int taskIndex;

  /** the update row splits */
  private List<RowUpdateSplit> rowsSplit;

  /** weather we need update the clock of the matrix partition */
  private boolean updateClock;

  /**
   * Create PutPartitionUpdateRequest.
   *
   * @param serverId parameter server id
   * @param taskIndex task index
   * @param clock clock value
   * @param partKey matrix partition key
   * @param rowsSplit update row splits
   * @param updateClock true means update the clock value of the matrix partition
   */
  public PutPartitionUpdateRequest(ParameterServerId serverId, int taskIndex, int clock,
      PartitionKey partKey, List<RowUpdateSplit> rowsSplit, boolean updateClock) {
    super(serverId, clock, partKey);
    this.setTaskIndex(taskIndex);
    this.setRowsSplit(rowsSplit);
    this.setUpdateClock(updateClock);
  }

  public PutPartitionUpdateRequest() {

  }

  @Override
  public int getEstimizeDataSize() {
    return bufferLen();
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.PUT_PARTUPDATE;
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
  public List<RowUpdateSplit> getRowsSplit() {
    return rowsSplit;
  }

  /**
   * Set update row splits.
   * 
   * @param rowsSplit update row splits.
   */
  public void setRowsSplit(List<RowUpdateSplit> rowsSplit) {
    this.rowsSplit = rowsSplit;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(taskIndex);
    buf.writeBoolean(updateClock);
    if (rowsSplit != null) {
      int size = rowsSplit.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        rowsSplit.get(i).serialize(buf);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    taskIndex = buf.readInt();
    updateClock = buf.readBoolean();
    rowsSplit = new ArrayList<>();
  }

  @Override
  public int bufferLen() {
    int len = 4;
    if(rowsSplit != null)  {
      int size = rowsSplit.size();
      for(int i = 0; i < size; i++) {
        len += rowsSplit.get(i).bufferLen();
      }
    }
    return len;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public void setTaskIndex(int taskIndex) {
    this.taskIndex = taskIndex;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((rowsSplit == null) ? 0 : rowsSplit.hashCode());
    result = prime * result + taskIndex;
    result = prime * result + (updateClock ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return "PutPartitionUpdateRequest [taskIndex=" + taskIndex + ", rowsSplit size="
        + rowsSplit.size() + ", updateClock=" + updateClock + ", toString()=" + super.toString()
        + "]";
  }

}
