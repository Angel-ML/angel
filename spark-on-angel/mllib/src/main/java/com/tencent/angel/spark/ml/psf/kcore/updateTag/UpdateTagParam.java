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
package com.tencent.angel.spark.ml.psf.kcore.updateTag;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UpdateTagParam extends UpdateParam {

  private final long[] nodeIds;

  private final int rowId;

  private final List<PartitionUpdateParam> partParams;

  public UpdateTagParam(int matrixId, long[] nodeIds, int rowId) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.rowId = rowId;
    this.partParams = new ArrayList<>();
  }

  public UpdateTagParam() {
    this(-1, null, 0);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }


  public int getRowId() {
    return rowId;
  }

  public List<PartitionUpdateParam> getPartParams() {
    return partParams;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    Arrays.sort(nodeIds);
    List<PartitionKey> partitions = PSAgentContext.get().getMatrixMetaManager()
        .getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(nodeIds, partitions)) {
      throw new AngelException(
          "node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
              .get(partitions.size() - 1).getEndCol());
    }
    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < nodeIds.length || partIndex < partitions.size()) {
      int length = 0;
      long endOffset = partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new PartUpdateTagParam(matrixId,
            partitions.get(partIndex), nodeIds, rowId, nodeIndex - length, nodeIndex));
      }
      partIndex++;
    }

    return partParams;
  }
}
