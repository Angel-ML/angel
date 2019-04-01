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

package com.tencent.angel.ml.matrix.psf.graph.adjacency.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class PartInitNeighborParam extends PartitionUpdateParam {

  private Map<Integer, int[]> nodeIdToNeighborIndices;
  private int[] nodeIds;
  private transient int startIndex;
  private transient int endIndex;

  public PartInitNeighborParam(int matrixId, PartitionKey partKey,
      Map<Integer, int[]> nodeIdToNeighborIndices, int[] nodeIds, int startIndex, int endIndex) {
    super(matrixId, partKey);
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
    this.nodeIds = nodeIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitNeighborParam() {
    this(0, null, null, null, 0, 0);
  }

  public Map<Integer, int[]> getNodeIdToNeighborIndices() {
    return nodeIdToNeighborIndices;
  }

  public void setNodeIdToNeighborIndices(Map<Integer, int[]> nodeIdToNeighborIndices) {
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
  }


  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int len = endIndex - startIndex;
    int nodeId;
    int[] neighbors;
    int writeIndex = buf.writerIndex();
    int writeNum = 0;
    buf.writeInt(0);
    for (int i = 0; i < len; i++) {
      nodeId = nodeIds[i];
      neighbors = nodeIdToNeighborIndices.get(nodeId);
      if (neighbors == null || neighbors.length == 0) {
        continue;
      }
      buf.writeInt(nodeId);
      buf.writeInt(neighbors.length);
      for (int neighbor : neighbors) {
        buf.writeInt(neighbor);
      }
      writeNum++;
    }
    buf.setInt(writeIndex, writeNum);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIdToNeighborIndices = new HashMap<>(len);

    for (int i = 0; i < len; i++) {
      int nodeId = buf.readInt();
      int neighborNum = buf.readInt();
      int[] neighbor = new int[neighborNum];
      for (int j = 0; j < neighborNum; j++) {
        neighbor[j] = buf.readInt();
      }
      nodeIdToNeighborIndices.put(nodeId, neighbor);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    for (Entry<Integer, int[]> entry : nodeIdToNeighborIndices.entrySet()) {
      if (entry.getValue() != null && entry.getValue().length > 0) {
        len += (8 + 4 * entry.getValue().length);
      }
    }
    return len;
  }
}
