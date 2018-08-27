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


package com.tencent.angel.ml.matrix.psf.get.getrow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Partition parameter of get row function.
 */
public class PartitionGetRowParam extends PartitionGetParam {
  /**
   * row index
   */
  private int rowIndex;

  /**
   * Create a new PartitionGetRowParam.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param partKey  matrix partition key
   */
  public PartitionGetRowParam(int matrixId, int rowIndex, PartitionKey partKey) {
    super(matrixId, partKey);
    this.rowIndex = rowIndex;
  }

  /**
   * Create a new PartitionGetRowParam.
   */
  public PartitionGetRowParam() {
    this(-1, -1, null);
  }

  /**
   * Get row index.
   *
   * @return int row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(rowIndex);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    rowIndex = buf.readInt();
  }

  @Override public int bufferLen() {
    return 4 + super.bufferLen();
  }

  @Override public String toString() {
    return "PartitionGetRowParam{" + "rowIndex=" + rowIndex + "} " + super.toString();
  }
}
