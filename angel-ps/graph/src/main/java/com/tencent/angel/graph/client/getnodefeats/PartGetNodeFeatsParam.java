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

package com.tencent.angel.graph.client.getnodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNodeFeatsParam extends PartitionGetParam {

  /**
   * Node ids
   */
  private int[] nodeIds;

  private int startIndex;
  private int endIndex;


  public PartGetNodeFeatsParam(int matrixId, PartitionKey part, int[] nodeIds
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartGetNodeFeatsParam() {
    this(-1, null, null, -1, -1);
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(int[] nodeIds) {
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
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeInt(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    nodeIds = new int[buf.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 4 * nodeIds.length;
  }
}
