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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.partition.op.ICSRStorageOp;
import io.netty.buffer.ByteBuf;

/**
 * Base class of CSR Storage
 */
public abstract class CSRStorage extends ServerPartitionStorage implements ICSRStorageOp {
  int [] rowOffsets;
  int [] columnIndices;

  public CSRStorage(int rowIdOffset) {
    super(rowIdOffset);
  }

  public void init(PartitionKey partKey) {
    rowOffsets = new int[(int)(partKey.getEndCol() - partKey.getStartCol() + 1)];
  }

  @Override
  public void init() {

  }

  @Override
  public void reset() {
    rowOffsets = new int[0];
    columnIndices = new int[0];
  }

  @Override
  public void update(ByteBuf buf, UpdateOp op) {
    // TODO
    throw new UnsupportedOperationException("CSR dose not support pipeline update now");
  }

  @Override
  public long getElemNum() {
    return columnIndices.length;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);

    // Serialize row offsets
    output.writeInt(rowOffsets.length);
    for(int i = 0; i < rowOffsets.length; i++) {
      output.writeInt(rowOffsets[i]);
    }

    // Serialize column offsets
    output.writeInt(columnIndices.length);
    for(int i = 0; i < columnIndices.length; i++) {
      output.writeInt(columnIndices[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);

    // Deserialize row offset
    int size = input.readInt();
    rowOffsets = new int[size];
    for(int i = 0; i < size; i++) {
      rowOffsets[i] = input.readInt();
    }

    // Deserialize row offset
    size = input.readInt();
    columnIndices = new int[size];
    for(int i = 0; i < size; i++) {
      columnIndices[i] = input.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + rowOffsets.length * 4 + 4 + columnIndices.length * 4;
  }

  public int[] getRowOffsets() {
    return rowOffsets;
  }

  public void setRowOffsets(int[] rowOffsets) {
    this.rowOffsets = rowOffsets;
  }

  public int[] getColumnIndices() {
    return columnIndices;
  }

  public void setColumnIndices(int[] columnIndices) {
    this.columnIndices = columnIndices;
  }
}
