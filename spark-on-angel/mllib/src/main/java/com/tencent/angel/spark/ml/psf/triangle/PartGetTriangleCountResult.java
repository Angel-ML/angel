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

package com.tencent.angel.spark.ml.psf.triangle;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetTriangleCount
 */
public class PartGetTriangleCountResult extends PartitionGetResult {

  private int partId;
  /**
   * Node id to triangle counts
   */
  private int[][] nodeIdToTriangleCounts;

  public PartGetTriangleCountResult(int partId, int[][] nodeIdToTriangleCounts) {
    this.partId = partId;
    this.nodeIdToTriangleCounts = nodeIdToTriangleCounts;
  }

  public PartGetTriangleCountResult() {
    this(-1, null);
  }

  public int[][] getNodeIdToTriangleCounts() {
    return nodeIdToTriangleCounts;
  }

  public void setNodeIdToTriangleCounts(
      int[][] nodeIdToTriangleCounts) {
    this.nodeIdToTriangleCounts = nodeIdToTriangleCounts;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(nodeIdToTriangleCounts.length);
    for (int i = 0; i < nodeIdToTriangleCounts.length; i++) {
      if (nodeIdToTriangleCounts[i] == null) {
        output.writeInt(0);
      } else {
        output.writeInt(nodeIdToTriangleCounts[i].length);
        for (int value : nodeIdToTriangleCounts[i]) {
          output.writeInt(value);
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int size = input.readInt();
    nodeIdToTriangleCounts = new int[size][];
    for (int i = 0; i < size; i++) {
      int[] neighbors = new int[input.readInt()];
      for (int j = 0; j < neighbors.length; j++) {
        neighbors[j] = input.readInt();
      }
      nodeIdToTriangleCounts[i] = neighbors;
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < nodeIdToTriangleCounts.length; i++) {
      if (nodeIdToTriangleCounts[i] == null) {
        len += 4;
      } else {
        len += 4;
        len += 4 * nodeIdToTriangleCounts[i].length;
      }
    }
    return len;
  }
}
