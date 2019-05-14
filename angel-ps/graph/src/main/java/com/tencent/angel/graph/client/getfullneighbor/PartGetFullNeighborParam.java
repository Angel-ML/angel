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

package com.tencent.angel.graph.client.getfullneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Parameters for part get full neighbors
 */
public class PartGetFullNeighborParam extends PartitionGetParam {

  /**
   * Node ids: it just a view for original node ids
   */
  private long[] nodeIds;

  /**
   * Edge types
   */
  private int[] edgeTypes;

  /**
   * Store position: start index in nodeIds
   */
  private int startIndex;

  /**
   * Store position: end index in nodeIds
   */
  private int endIndex;

  public PartGetFullNeighborParam(int matrixId, PartitionKey part, long[] nodeIds, int[] edgeTypes
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.edgeTypes = edgeTypes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartGetFullNeighborParam() {
    this(0, null, null, null, 0, 0);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
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
      buf.writeLong(nodeIds[i]);
    }

    if (edgeTypes == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(edgeTypes.length);
      for (int i = 0; i < edgeTypes.length; i++) {
        buf.writeInt(edgeTypes[i]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIds = new long[len];
    for (int i = 0; i < len; i++) {
      nodeIds[i] = buf.readLong();
    }

    len = buf.readInt();
    edgeTypes = new int[len];
    for (int i = 0; i < len; i++) {
      edgeTypes[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 8 * nodeIds.length + 4 + ((edgeTypes == null) ? 0
        : 4 * edgeTypes.length);
  }
}
