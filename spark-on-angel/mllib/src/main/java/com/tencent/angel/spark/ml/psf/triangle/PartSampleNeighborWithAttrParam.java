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

package com.tencent.angel.spark.ml.psf.triangle;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Partition Sample parameter of SampleNeighborWithByteAttr
 */
public class PartSampleNeighborWithAttrParam extends PartitionGetParam {

  /**
   * Node ids
   */
  private long[] nodeIds;

  /**
   * Sample number, if count <= 0, means return all neighbors
   */
  private int count;

  private int startIndex;

  private int endIndex;

  public PartSampleNeighborWithAttrParam(int matrixId, PartitionKey part, int count, long[] nodeIds
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.count = count;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartSampleNeighborWithAttrParam() {
    this(0, null, 0, null, 0, 0);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
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
    buf.writeInt(count);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = buf.readInt();
    nodeIds = new long[buf.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 8 * nodeIds.length;
  }
}
