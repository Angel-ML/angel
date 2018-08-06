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
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.matrix.transport.adapter.RowsUpdateSplit;
import io.netty.buffer.ByteBuf;

public class PlusRowsRequest extends PutPartitionUpdateRequest {
  /** the update row splits */
  private RowsUpdateSplit rowsSplit;

  /**
   * Create PutPartitionUpdateRequest.
   *
   * @param taskIndex task index
   * @param clock clock value
   * @param partKey matrix partition key
   * @param rowsSplit update row splits
   * @param updateClock true means update the clock value of the matrix partition
   */
  public PlusRowsRequest(int taskIndex, int clock,
    PartitionKey partKey, RowsUpdateSplit rowsSplit, boolean updateClock) {
    super(taskIndex, clock, partKey, null, updateClock);
    this.rowsSplit = rowsSplit;
  }

  public PlusRowsRequest() {
    super(-1, -1, null, null, false);
  }

  @Override
  public int getEstimizeDataSize() {
    return bufferLen();
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.PUT_PARTUPDATE;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowsSplit != null) {
      rowsSplit.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    rowsSplit = new RowsUpdateSplit();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    if(rowsSplit != null)  {
      len += rowsSplit.bufferLen();
    }
    return len;
  }
}
