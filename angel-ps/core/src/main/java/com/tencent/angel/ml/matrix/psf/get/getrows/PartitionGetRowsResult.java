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


package com.tencent.angel.ml.matrix.psf.get.getrows;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of get row function for a matrix partition.
 */
public class PartitionGetRowsResult extends PartitionGetResult {
  /**
   * row splits
   */
  private List<ServerRow> rowSplits;

  /**
   * Create a new PartitionGetRowsResult.
   *
   * @param rowSplits row splits
   */
  public PartitionGetRowsResult(List<ServerRow> rowSplits) {
    this.rowSplits = rowSplits;
  }

  /**
   * Create a new PartitionGetRowsResult.
   */
  public PartitionGetRowsResult() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {
    if (rowSplits == null) {
      buf.writeInt(0);
    } else {
      int size = rowSplits.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowSplits.get(i).getRowType().getNumber());
        rowSplits.get(i).serialize(buf);
      }
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    int size = buf.readInt();
    rowSplits = new ArrayList<ServerRow>(size);
    for (int i = 0; i < size; i++) {
      RowType type = RowType.valueOf(buf.readInt());
      ServerRow rowSplit = ServerRowFactory.createEmptyServerRow(type);
      rowSplit.deserialize(buf);
      rowSplits.add(rowSplit);
    }
  }

  @Override public int bufferLen() {
    int len = 4;
    if (rowSplits != null) {
      int size = rowSplits.size();
      for (int i = 0; i < size; i++) {
        len += 4 + rowSplits.get(i).bufferLen();
      }
    }

    return len;
  }

  /**
   * Get row splits.
   *
   * @return List<ServerRow> row splits
   */
  public List<ServerRow> getRowSplits() {
    return rowSplits;
  }
}
