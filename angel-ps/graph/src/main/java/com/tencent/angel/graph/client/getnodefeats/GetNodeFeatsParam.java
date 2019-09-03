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
package com.tencent.angel.graph.client.getnodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetNodeFeatsParam extends GetParam {

  /**
   * Node ids
   */
  private final int[] nodeIds;

  private final List<PartitionGetParam> partParams;

  public GetNodeFeatsParam(int matrixId, int[] nodeIds) {
    super(matrixId);
    this.nodeIds = nodeIds;
    partParams = new ArrayList<>();
  }

  public GetNodeFeatsParam() {
    this(-1, null);
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public List<PartitionGetParam> getPartParams() {
    return partParams;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(nodeIds);

    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(nodeIds, partitions)) {
      throw new AngelException(
          "node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
              .get(partitions.size() - 1).getEndCol());
    }

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
        partParams.add(new PartGetNodeFeatsParam(matrixId,
            partitions.get(partIndex), nodeIds, nodeIndex - length, nodeIndex));
      }
      partIndex++;
    }

    return partParams;
  }
}
