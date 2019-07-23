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


package com.tencent.angel.matrix.psf.get.getrow;

import com.tencent.angel.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.servingmath2.utils.RowType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;

/**
 * The result of partition get row function.
 */
public class PartitionGetRowResult extends PartitionGetResult {
  /**
   * row split
   */
  private ServerRow rowSplit;

  /**
   * Create a new PartitionGetRowResult.
   *
   * @param rowSplit row split
   */
  public PartitionGetRowResult(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }

  /**
   * Create a new PartitionGetRowResult.
   */
  public PartitionGetRowResult() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {
    if (rowSplit != null) {
      buf.writeInt(rowSplit.getRowType().getNumber());
      rowSplit.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    if (buf.readableBytes() == 0) {
      rowSplit = null;
      return;
    }

    RowType type = RowType.valueOf(buf.readInt());
    rowSplit = ServerRowFactory.createEmptyServerRow(type);
    rowSplit.deserialize(buf);
  }

  @Override public int bufferLen() {
    if (rowSplit != null) {
      return 4 + rowSplit.bufferLen();
    } else {
      return 0;
    }
  }

  /**
   * Get row split.
   *
   * @return ServerRow row split
   */
  public ServerRow getRowSplit() {
    return rowSplit;
  }
}
