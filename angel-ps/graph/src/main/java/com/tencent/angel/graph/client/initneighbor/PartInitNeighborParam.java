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

package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Partition parameter for InitNeighbor function
 */
public class PartInitNeighborParam extends PartitionUpdateParam {

  /**
   * Node ids
   */
  private int[] nodeIds;

  /**
   * Node id to neighbors map, it it just used in PSClient
   */
  private Int2ObjectOpenHashMap<int[]> nodeIdToNeighbors;

  /**
   * Partition range start index for nodeIds, it is just used in PSClient
   */
  private transient int startPos;

  /**
   * Partition range end index for nodeIds, it is just used in PSClient
   */
  private transient int endPos;

  /**
   * Node neighbors, it is just used in PS
   */
  private int[] neighbors;

  /**
   * Node neighbor number, it is just used in PS
   */
  private int[] neighborNums;

  public PartInitNeighborParam(int matrixId, PartitionKey partKey, int[] nodeIds,
      Int2ObjectOpenHashMap<int[]> nodeIdToNeighbors,
      int startPos, int endPos) {
    super(matrixId, partKey, false);
    this.nodeIds = nodeIds;
    this.nodeIdToNeighbors = nodeIdToNeighbors;
    this.startPos = startPos;
    this.endPos = endPos;
  }

  public PartInitNeighborParam() {
    this(-1, null, null, null, 0, 0);
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(int[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public int[] getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(int[] neighbors) {
    this.neighbors = neighbors;
  }

  public int[] getNeighborNums() {
    return neighborNums;
  }

  public void setNeighborNums(int[] neighborNums) {
    this.neighborNums = neighborNums;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(endPos - startPos);
    for (int i = startPos; i < endPos; i++) {
      buf.writeInt(nodeIds[i]);
      buf.writeInt(nodeIdToNeighbors.get(nodeIds[i]).length);
    }

    for (int i = startPos; i < endPos; i++) {
      int[] neighbors = nodeIdToNeighbors.get(nodeIds[i]);
      for (int j = 0; j < neighbors.length; j++) {
        buf.writeInt(neighbors[j]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIds = new int[len];
    neighborNums = new int[len];

    int totalNum = 0;
    for (int i = 0; i < len; i++) {
      nodeIds[i] = buf.readInt();
      neighborNums[i] = buf.readInt();
      totalNum += neighborNums[i];
    }

    neighbors = new int[totalNum];

    for (int i = 0; i < totalNum; i++) {
      neighbors[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startPos; i < endPos; i++) {
      len += 8;
      len += nodeIdToNeighbors.get(nodeIds[i]).length * 4;
    }
    return len;
  }
}
