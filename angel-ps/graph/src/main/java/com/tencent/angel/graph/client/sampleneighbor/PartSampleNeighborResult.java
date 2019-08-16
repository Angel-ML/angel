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

package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Map.Entry;

/**
 * Result of GetNeighbor
 */
public class PartSampleNeighborResult extends PartitionGetResult {

  /**
   * Node id to neighbors map
   */
  private Int2ObjectOpenHashMap<int[]> nodeIdToNeighbors;

  public PartSampleNeighborResult(Int2ObjectOpenHashMap<int[]> nodeIdToNeighbors) {
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }

  public PartSampleNeighborResult() {
    this(null);
  }

  public Int2ObjectOpenHashMap<int[]> getNodeIdToNeighbors() {
    return nodeIdToNeighbors;
  }

  public void setNodeIdToNeighbors(
      Int2ObjectOpenHashMap<int[]> nodeIdToNeighbors) {
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(nodeIdToNeighbors.size());
    ObjectIterator<Int2ObjectMap.Entry<int[]>> iter = nodeIdToNeighbors
        .int2ObjectEntrySet().fastIterator();
    Int2ObjectMap.Entry<int[]> entry;
    while (iter.hasNext()) {
      entry = iter.next();
      output.writeInt(entry.getIntKey());
      int[] values = entry.getValue();
      output.writeInt(values.length);
      for (int value : values) {
        output.writeInt(value);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int size = input.readInt();
    nodeIdToNeighbors = new Int2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      int nodeIndex = input.readInt();
      int[] neighbors = new int[input.readInt()];
      for (int j = 0; j < neighbors.length; j++) {
        neighbors[j] = input.readInt();
      }
      nodeIdToNeighbors.put(nodeIndex, neighbors);
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    for (Entry<Integer, int[]> entry : nodeIdToNeighbors.entrySet()) {
      len += 8;
      len += entry.getValue().length * 4;
    }
    return len;
  }
}
