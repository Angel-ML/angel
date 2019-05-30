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

package com.tencent.angel.graph.client;

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

public class NodeIDWeightPairs implements Serialize {

  /**
   * Edge types
   */
  int[] edgeTypes;

  /**
   * Neighbor node ids
   */
  long[] neighborNodeIds;

  /**
   * Neighbor node weights
   */
  float[] neighborNodeWeights;

  public NodeIDWeightPairs(int[] edgeTypes, long[] neighborNodeIds, float[] neighborNodeWeights) {
    this.edgeTypes = edgeTypes;
    this.neighborNodeIds = neighborNodeIds;
    this.neighborNodeWeights = neighborNodeWeights;
  }

  public NodeIDWeightPairs() {
    this(null, null, null);
  }


  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(int[] edgeTypes) {
    this.edgeTypes = edgeTypes;
  }

  public long[] getNeighborNodeIds() {
    return neighborNodeIds;
  }

  public void setNeighborNodeIds(long[] neighborNodeIds) {
    this.neighborNodeIds = neighborNodeIds;
  }

  public float[] getNeighborNodeWeights() {
    return neighborNodeWeights;
  }

  public void setNeighborNodeWeights(float[] neighborNodeWeights) {
    this.neighborNodeWeights = neighborNodeWeights;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(edgeTypes.length);
    for (int i = 0; i < edgeTypes.length; i++) {
      output.writeInt(edgeTypes[i]);
    }

    output.writeInt(neighborNodeIds.length);
    for (int i = 0; i < neighborNodeIds.length; i++) {
      output.writeLong(neighborNodeIds[i]);
    }

    output.writeInt(neighborNodeWeights.length);
    for (int i = 0; i < neighborNodeWeights.length; i++) {
      output.writeFloat(neighborNodeWeights[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    edgeTypes = new int[input.readInt()];
    for (int i = 0; i < edgeTypes.length; i++) {
      edgeTypes[i] = input.readInt();
    }

    neighborNodeIds = new long[input.readInt()];
    for (int i = 0; i < neighborNodeIds.length; i++) {
      neighborNodeIds[i] = input.readLong();
    }

    neighborNodeWeights = new float[input.readInt()];
    for (int i = 0; i < neighborNodeWeights.length; i++) {
      neighborNodeWeights[i] = input.readFloat();
    }
  }

  @Override
  public int bufferLen() {
    return (4 + 4 * edgeTypes.length) + (4 + 8 * neighborNodeIds.length) + (4
        + 4 * neighborNodeWeights.length);
  }
}
