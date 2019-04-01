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

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import com.tencent.angel.ps.storage.vector.element.IntArrayElement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;

/**
 * Get neighbor for a batch of nodes
 */
public class GetNeighbor extends GetFunc {

  public GetNeighbor(GetNeighborParam param) {
    super(param);
  }

  public GetNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetNeighborParam param = (PartGetNeighborParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerIntAnyRow row = (ServerIntAnyRow)part.getRow(0);
    IntArrayElement element;
    Map<Integer, int[]> results = new HashMap<>();

    for(int i = 0; i < param.getNodeIndices().length; i++) {
      int nodeIndex = param.getNodeIndices()[i];
      element = (IntArrayElement) row.get(nodeIndex);
      if(element != null) {
        if(param.getGetNeighborNum() < 0 || element.getData().length <= param.getGetNeighborNum()) {
          results.put(nodeIndex, element.getData());
        } else {
          int [] neighbors = new int[param.getGetNeighborNum()];
          System.arraycopy(element.getData(), 0, neighbors[i], 0, param.getGetNeighborNum());
          results.put(nodeIndex, neighbors);
        }
      }
    }

    return new PartGetNeighborResult(results);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int len = 0;
    for(PartitionGetResult result : partResults) {
      PartGetNeighborResult getNeighborResult = (PartGetNeighborResult) result;
      len += getNeighborResult.getNeighborIndices().size();
    }

    Map<Integer, int[]> neighbors = new HashedMap(len);
    for(PartitionGetResult result : partResults) {
      PartGetNeighborResult getNeighborResult = (PartGetNeighborResult) result;
      neighbors.putAll(getNeighborResult.getNeighborIndices());
    }

    return new GetNeighborResult(neighbors);
  }
}
