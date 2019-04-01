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

package com.tencent.angel.ml.matrix.psf.graph.adjacency.getneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNeighborParam extends PartitionGetParam {

  private int[] nodeIndices;
  private int getNeighborNum;
  private int startIndex;
  private int endIndex;

  public PartGetNeighborParam(int matrixId, PartitionKey part, int getNeighborNum, int[] nodeIndices
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIndices = nodeIndices;
    this.getNeighborNum = getNeighborNum;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartGetNeighborParam() {
    this(0, null, 0, null, 0, 0);
  }

  public int[] getNodeIndices() {
    return nodeIndices;
  }

  public void setNodeIndices(int[] nodeIndices) {
    this.nodeIndices = nodeIndices;
  }

  public int getGetNeighborNum() {
    return getNeighborNum;
  }

  public void setGetNeighborNum(int getNeighborNum) {
    this.getNeighborNum = getNeighborNum;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(getNeighborNum);
    buf.writeInt(startIndex);
    buf.writeInt(endIndex);

    for (int i = startIndex; i < endIndex; i++) {
      buf.writeInt(nodeIndices[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    getNeighborNum = buf.readInt();
    startIndex = buf.readInt();
    endIndex = buf.readInt();

    int len = endIndex - startIndex;
    nodeIndices = new int[len];
    for (int i = 0; i < len; i++) {
      nodeIndices[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 12 + 4 * nodeIndices.length;
  }
}
