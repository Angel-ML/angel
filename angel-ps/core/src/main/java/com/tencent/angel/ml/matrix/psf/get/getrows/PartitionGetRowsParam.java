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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Partition parameter of get rows function.
 */
public class PartitionGetRowsParam extends PartitionGetParam {
  /**
   * row indexes
   */
  private List<Integer> rowIndexes;

  /**
   * Create a new PartitionGetRowsParam.
   *
   * @param matrixId   matrix id
   * @param partKey    matrix partition key
   * @param rowIndexes row indexes
   */
  public PartitionGetRowsParam(int matrixId, PartitionKey partKey, List<Integer> rowIndexes) {
    super(matrixId, partKey);
    this.setRowIndexes(rowIndexes);
  }

  /**
   * Create a new PartitionGetRowsParam.
   */
  public PartitionGetRowsParam() {
    this(-1, null, null);
  }

  /**
   * Get row indexes.
   *
   * @return List<Integer> row indexes
   */
  public List<Integer> getRowIndexes() {
    return rowIndexes;
  }

  /**
   * Set row indexes.
   *
   * @param rowIndexes row indexes
   */
  public void setRowIndexes(List<Integer> rowIndexes) {
    this.rowIndexes = rowIndexes;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowIndexes != null) {
      int size = rowIndexes.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowIndexes.get(i));
      }
    } else {
      buf.writeInt(0);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = buf.readInt();
    rowIndexes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      rowIndexes.add(buf.readInt());
    }
  }

  @Override public int bufferLen() {
    return 4 + super.bufferLen() + ((rowIndexes != null) ? rowIndexes.size() * 4 : 0);
  }
}
