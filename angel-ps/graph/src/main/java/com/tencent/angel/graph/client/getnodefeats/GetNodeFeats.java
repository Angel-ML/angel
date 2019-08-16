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

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.List;

public class GetNodeFeats extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public GetNodeFeats(GetNodeFeatsParam param) {
    super(param);
  }

  public GetNodeFeats() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetNodeFeatsParam param = (PartGetNodeFeatsParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerIntAnyRow row = (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
    int[] nodeIds = param.getNodeIds();

    IntFloatVector[] feats = new IntFloatVector[nodeIds.length];
    for (int i = 0; i < nodeIds.length; i++) {
      if (row.get(nodeIds[i]) == null) {
        continue;
      }
      feats[i] = ((Node) (row.get(nodeIds[i]))).getFeats();
    }
    return new PartGetNodeFeatsResult(part.getPartitionKey().getPartitionId(), feats);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
        partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartGetNodeFeatsResult) result).getPartId(), result);
    }

    GetNodeFeatsParam param = (GetNodeFeatsParam) getParam();
    int[] nodeIds = param.getNodeIds();
    List<PartitionGetParam> partParams = param.getPartParams();

    Int2ObjectOpenHashMap<IntFloatVector> results = new Int2ObjectOpenHashMap<>(nodeIds.length);

    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      PartGetNodeFeatsParam partParam = (PartGetNodeFeatsParam) partParams.get(i);
      PartGetNodeFeatsResult partResult = (PartGetNodeFeatsResult) partIdToResultMap
          .get(partParam.getPartKey().getPartitionId());

      int start = partParam.getStartIndex();
      int end = partParam.getEndIndex();
      IntFloatVector[] feats = partResult.getFeats();
      for (int j = start; j < end; j++) {
        results.put(nodeIds[j], feats[j - start]);
      }
    }
    return new GetNodeFeatsResult(results);
  }
}
