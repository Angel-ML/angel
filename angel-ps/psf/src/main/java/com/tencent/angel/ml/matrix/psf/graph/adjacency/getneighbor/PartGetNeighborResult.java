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

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.collections.map.HashedMap;

public class PartGetNeighborResult extends PartitionGetResult {

  private Map<Integer, int []> neighborIndices;

  PartGetNeighborResult(Map<Integer, int []> neighborIndices) {
    this.neighborIndices = neighborIndices;
  }

  public PartGetNeighborResult() {
    this(null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(neighborIndices.size());
    for(Entry<Integer, int[]> entry : neighborIndices.entrySet()) {
      buf.writeInt(entry.getKey());
      int [] values = entry.getValue();
      buf.writeInt(values.length);
      for(int value : values) {
        buf.writeInt(value);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int size = buf.readInt();
    neighborIndices = new HashedMap(size);
    for(int i = 0; i < size; i++) {
      int nodeIndex = buf.readInt();
      int [] neighbors = new int[buf.readInt()];
      for(int j = 0; j < neighbors.length; j++) {
        neighbors[j] = buf.readInt();
      }
      neighborIndices.put(nodeIndex, neighbors);
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    for(Entry<Integer, int[]> entry : neighborIndices.entrySet()) {
      len += 8;
      len += entry.getValue().length * 4;
    }
    return len;
  }

  public Map<Integer, int[]> getNeighborIndices() {
    return neighborIndices;
  }

  public void setNeighborIndices(Map<Integer, int[]> neighborIndices) {
    this.neighborIndices = neighborIndices;
  }
}
