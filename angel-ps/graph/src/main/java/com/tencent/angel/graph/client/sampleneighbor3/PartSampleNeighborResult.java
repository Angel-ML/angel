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

package com.tencent.angel.graph.client.sampleneighbor3;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetNeighbor
 */
public class PartSampleNeighborResult extends PartitionGetResult {

  private int partId;
  /**
   * Node id to neighbors map
   */
  private long[][] nodeIdToNeighbors;

  public PartSampleNeighborResult(int partId, long[][] nodeIdToNeighbors) {
    this.partId = partId;
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }

  public PartSampleNeighborResult() {
    this(-1, null);
  }

  public long[][] getNodeIdToNeighbors() {
    return nodeIdToNeighbors;
  }

  public void setNodeIdToNeighbors(
      long[][] nodeIdToNeighbors) {
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(nodeIdToNeighbors.length);
    for (int i = 0; i < nodeIdToNeighbors.length; i++) {
      if (nodeIdToNeighbors[i] == null) {
        output.writeInt(0);
      } else {
        output.writeInt(nodeIdToNeighbors[i].length);
        for (long value : nodeIdToNeighbors[i]) {
          output.writeLong(value);
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int size = input.readInt();
    nodeIdToNeighbors = new long[size][];
    for (int i = 0; i < size; i++) {
      long[] neighbors = new long[input.readInt()];
      for (int j = 0; j < neighbors.length; j++) {
        neighbors[j] = input.readLong();
      }
      nodeIdToNeighbors[i] = neighbors;
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < nodeIdToNeighbors.length; i++) {
      if (nodeIdToNeighbors[i] == null) {
        len += 4;
      } else {
        len += 4;
        len += 8 * nodeIdToNeighbors[i].length;
      }
    }
    return len;
  }
}
