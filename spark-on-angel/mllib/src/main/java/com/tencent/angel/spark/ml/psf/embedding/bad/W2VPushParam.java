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

package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class W2VPushParam extends UpdateParam {

  int[] indices;
  float[] deltas;
  int numNodePerRow;
  int dimension;


  public W2VPushParam(int matrixId, int[] indices, float[] deltas, int numNodePerRow, int dimension) {
    super(matrixId);
    this.indices = indices;
    this.deltas = deltas;
    this.numNodePerRow = numNodePerRow;
    this.dimension = dimension;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionUpdateParam> params = new ArrayList<>();

    int start = 0, end = 0;
    for (PartitionKey pkey: pkeys) {
      int startRow = pkey.getStartRow();
      int endRow   = pkey.getEndRow();
      int startNode = startRow * numNodePerRow;
      int endNode  = endRow * numNodePerRow;

      if (start < indices.length && indices[start] >= startNode) {
        while (end < indices.length && indices[end] < endNode)
          end++;

        if (end > start)
          params.add(new W2VPushPartitionParam(matrixId,
                  pkey,
                  indices,
                  deltas,
                  numNodePerRow,
                  start,
                  end - start,
                  dimension));
        start = end;
      }
    }

    return params;
  }
}
