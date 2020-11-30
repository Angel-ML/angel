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
package com.tencent.angel.graph.client.node2vec.params;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

abstract public class GetParamWithKeyIds extends GetParam {
  protected long[] keyIds;  // pls. make sure keyIds is sorted

  public GetParamWithKeyIds(int matrixId, long[] keyIds) {
    super(matrixId);
    this.keyIds = keyIds;
  }

  public GetParamWithKeyIds(long[] keyIds) {
    super();
    this.keyIds = keyIds;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<>(size);

    int nodeIndex = 0;
    for (PartitionKey part : parts) {
      int start = nodeIndex;  // include start
      while (nodeIndex < keyIds.length && keyIds[nodeIndex] < part.getEndCol()) {
        nodeIndex++;
      }
      int end = nodeIndex;  // exclude end

      if (end - start > 0) {
        partParams.add(getPartitionParam(part, start, end));
      }
    }

    return partParams;
  }

  protected abstract PartitionGetParam getPartitionParam(PartitionKey part, int startIdx, int endIdx);
}
