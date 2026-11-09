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

package com.tencent.angel.spark.ml.psf.kcore;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetNeighbor
 */
public class PartGetCoresResult extends PartitionGetResult {

  private int partId;
  /**
   * Node id to neighbors map
   */
  private int[] cores;
  private long[] nodes;

  public PartGetCoresResult(int partId, long[] nodes, int[] cores) {
    this.partId = partId;
    this.nodes = nodes;
    this.cores = cores;
  }

  public PartGetCoresResult() {
    this(-1, null, null);
  }

  public int[] getCores() {
    return cores;
  }

  public long[] getNodes() {
    return nodes;
  }

  public void setNodeIdToNeighbors(
      int[] cores, long[] nodes) {
    this.cores = cores;
    this.nodes = nodes;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(cores.length);
    for (int i = 0; i < cores.length; i++) {
      output.writeInt(cores[i]);
    }

    output.writeInt(nodes.length);
    for (int i = 0; i < nodes.length; i++) {
      output.writeLong(nodes[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int size = input.readInt();
    cores = new int[size];
    for (int i = 0; i < size; i++) {
      cores[i] = input.readInt();
    }
    int size_ = input.readInt();
    nodes = new long[size_];
    for (int i = 0; i < size_; i++) {
      nodes[i] = input.readLong();
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    len += 4 * cores.length;
    len += 4;
    len += 8 * nodes.length;
    return len;
  }
}
