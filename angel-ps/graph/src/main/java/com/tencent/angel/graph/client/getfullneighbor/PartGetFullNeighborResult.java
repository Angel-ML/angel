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

import com.tencent.angel.graph.client.NodeIDWeightPairs;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Results for part get full neighbors
 */
public class PartGetFullNeighborResult extends PartitionGetResult {

  private int partId;
  private NodeIDWeightPairs[] nodeIdsWeights;
  private static final int NullMark = 0;
  private static final int NotNullMark = 1;

  PartGetFullNeighborResult(int partId, NodeIDWeightPairs[] nodeIdsWeights) {
    this.partId = partId;
    this.nodeIdsWeights = nodeIdsWeights;
  }

  public PartGetFullNeighborResult() {
    this(-1, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(partId);
    buf.writeInt(nodeIdsWeights.length);
    for (int i = 0; i < nodeIdsWeights.length; i++) {
      if (nodeIdsWeights[i] == null) {
        buf.writeInt(NullMark);
      } else {
        buf.writeInt(NotNullMark);
        nodeIdsWeights[i].serialize(buf);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partId = buf.readInt();
    int size = buf.readInt();
    nodeIdsWeights = new NodeIDWeightPairs[size];
    for (int i = 0; i < size; i++) {
      int mark = buf.readInt();
      if(mark == NotNullMark) {
        nodeIdsWeights[i] = new NodeIDWeightPairs();
        nodeIdsWeights[i].deserialize(buf);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < nodeIdsWeights.length; i++) {
      if (nodeIdsWeights[i] == null) {
        len += 4;
      } else {
        len += 4;
        len += nodeIdsWeights[i].bufferLen();
      }
    }

    return len;
  }

  public NodeIDWeightPairs[] getNeighborIndices() {
    return nodeIdsWeights;
  }

  public int getPartId() {
    return partId;
  }
}
