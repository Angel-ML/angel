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

package com.tencent.angel.spark.ml.psf.kcore.updateTag;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

/**
 * Partition Sample parameter of SampleNeighbor
 */
public class PartUpdateTagParam extends PartitionUpdateParam {

  /**
   * Node ids
   */
  private long[] nodeIds;

  private int rowId;
  /**
   * Sample number, if count <= 0, means return all neighbors
   */

  private int startIndex;

  private int endIndex;

  public PartUpdateTagParam(int matrixId, PartitionKey part, long[] nodeIds,
      int rowId, int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.rowId = rowId;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartUpdateTagParam() {
    this(0, null, null, 0, 0, 0);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int getRowId() {
    return rowId;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(rowId);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    rowId = buf.readInt();
    nodeIds = new long[buf.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 8 + 8 * (endIndex - startIndex);
  }
}
