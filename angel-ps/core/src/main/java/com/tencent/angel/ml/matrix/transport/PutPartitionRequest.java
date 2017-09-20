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

import java.util.List;

public class PutPartitionRequest extends PartitionRequest {
  private List<RowUpdateSplit> rowsSplit;
  private boolean updateClock;

  public PutPartitionRequest(ParameterServerId serverId, int clock, PartitionKey partKey,
      List<RowUpdateSplit> rowsSplit, boolean updateClock) {
    super(serverId, clock, partKey);
    this.setRowsSplit(rowsSplit);
    this.setUpdateClock(updateClock);
  }

  @Override
  public int getEstimizeDataSize() {
    return bufferLen();
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.PUT_PART;
  }

  public boolean isUpdateClock() {
    return updateClock;
  }

  public void setUpdateClock(boolean updateClock) {
    this.updateClock = updateClock;
  }

  public List<RowUpdateSplit> getRowsSplit() {
    return rowsSplit;
  }

  public void setRowsSplit(List<RowUpdateSplit> rowsSplit) {
    this.rowsSplit = rowsSplit;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBoolean(updateClock);
    if (rowsSplit != null) {
      int size = rowsSplit.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowsSplit.get(i).getRowType().getNumber());
        rowsSplit.get(i).serialize(buf);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    // TODO:
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    if(rowsSplit != null)  {
      int size = rowsSplit.size();
      for(int i = 0; i < size; i++) {
        len += rowsSplit.get(i).bufferLen();
      }
    }
    return len;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((rowsSplit == null) ? 0 : rowsSplit.hashCode());
    result = prime * result + (updateClock ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public String toString() {
    return "PutPartitionRequest [rowsSplit size=" + rowsSplit.size() + ", updateClock="
        + updateClock + ", toString()=" + super.toString() + "]";
  }
}
