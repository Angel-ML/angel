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

package com.tencent.angel.graph.model.neighbor.dynamic.psf.get;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetNeighbor
 */
public class PartGetNeighborResult extends PartitionGetResult {

  /**
   * Node id to neighbors map
   */
  private byte[][] neighbors;
  private long[] nodes;

  public PartGetNeighborResult(long[] nodes, byte[][] neighbors) {
    this.nodes = nodes;
    this.neighbors = neighbors;
  }

  public PartGetNeighborResult() {
    this(null, null);
  }

  public long[] getNodes() {
    return nodes;
  }

  public byte[][] getNeighbors() {
    return neighbors;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodes);
    ByteBufSerdeUtils.serializeInt(output, neighbors.length);
    for (int i = 0; i < neighbors.length; i++) {
      ByteBufSerdeUtils.serializeBytes(output, neighbors[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodes = ByteBufSerdeUtils.deserializeLongs(input);
    int num = ByteBufSerdeUtils.deserializeInt(input);
    neighbors = new byte[num][];
    for (int i = 0; i < num; i++) {
      neighbors[i] = ByteBufSerdeUtils.deserializeBytes(input);
    }
  }

  @Override
  public int bufferLen() {
    int len = ByteBufSerdeUtils.serializedLongsLen(nodes);
    len += 4;
    for (int i = 0; i < neighbors.length; i++) {
      len += ByteBufSerdeUtils.serializedBytesLen(neighbors[i]);
    }
    return len;
  }
}
