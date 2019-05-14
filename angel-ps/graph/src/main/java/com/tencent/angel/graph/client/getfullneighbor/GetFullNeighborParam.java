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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parameters for GetNeighbor
 */
public class GetFullNeighborParam extends GetParam {
  /**
   * Node ids the need get neighbors
   */
  private long[] nodeIds;

  /**
   * Edge types
   */
  private int[] edgeTypes;

  /**
   * Just store the split positions
   */
  private transient int [] offsets;

  public GetFullNeighborParam(int matrixId, long[] nodeIds, int[] edgeTypes) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.edgeTypes = edgeTypes;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Sort the node
    Arrays.sort(nodeIds);

    List<PartitionGetParam> partParams = new ArrayList<>();
    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    offsets = new int[partitions.size()];

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < nodeIds.length || partIndex < partitions.size()) {
      int length = 0;
      int endOffset = (int) partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new PartGetFullNeighborParam(matrixId,
            partitions.get(partIndex), nodeIds, edgeTypes, nodeIndex - length, nodeIndex));
      }
      offsets[partIndex] = nodeIndex;
      partIndex++;
    }

    return partParams;
  }
}
