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
public class GetNeighborParam extends GetParam {

  /**
   * Node ids the need get nerghbors
   */
  private int[] nodeIndices;

  /**
   * Max neighbor number return for each node, if < 0, means return all neighbors
   */
  private int getNeighborNum;

  public GetNeighborParam(int matrixId, int[] nodeIndices, int getNeighborNum) {
    super(matrixId);
    this.nodeIndices = nodeIndices;
    this.getNeighborNum = getNeighborNum;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(nodeIndices);

    List<PartitionGetParam> partParams = new ArrayList<>();
    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < nodeIndices.length || partIndex < partitions.size()) {
      int length = 0;
      int endOffset = (int) partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIndices.length && nodeIndices[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new PartGetNeighborParam(matrixId,
            partitions.get(partIndex), getNeighborNum, nodeIndices, nodeIndex - length, nodeIndex));
      }
      partIndex++;
    }

    return partParams;
  }
}
