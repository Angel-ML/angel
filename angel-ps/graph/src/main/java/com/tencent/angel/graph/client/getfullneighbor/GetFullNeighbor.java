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
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get all neighbor for a batch of nodes
 */
public class GetFullNeighbor extends GetFunc {

  /**
   * Create GetFullNeighbor
   * @param param parameter
   */
  public GetFullNeighbor(GetFullNeighborParam param) {
    super(param);
  }

  /**
   * Create a empty GetFullNeighbor
   */
  public GetFullNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetFullNeighborParam param = (PartGetFullNeighborParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition)matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow)part.getRow(0);

    // Results
    NodeIDWeightPairs[] results = new NodeIDWeightPairs[param.getNodeIds().length];

    // Get neighbors for each node
    long [] nodeIds = param.getNodeIds();
    for(int i = 0; i < nodeIds.length; i++) {
      results[i] = getNeighbors((Node) row.get(nodeIds[i]), param.getEdgeTypes());
    }

    return new PartGetFullNeighborResult(part.getPartitionKey().getPartitionId(), results);
  }

  private NodeIDWeightPairs getNeighbors(Node node, int [] edgeTypes) {
    if(node != null) {
      // Total neighbors number of valid edge types for this node
      int neighborsNum = 0;

      // First get result number
      for(int i = 0; i < edgeTypes.length; i++) {
        int edgeType = edgeTypes[i];
        if(edgeType >= 0 && edgeType < node.getEdgeTypes().length) {
          int startIndex = edgeType > 0 ? node.getNeigborGroupIndices()[i - 1] : 0;
          int endIndex = node.getNeigborGroupIndices()[edgeType];
          neighborsNum += (endIndex - startIndex);
        }
      }

      // Valid edge types
      int [] validEdgeTypes = new int[neighborsNum];

      // Neighbors
      long [] neighborNodeIds = new long[neighborsNum];

      // Neighbors weights
      float [] neighborWeights = new float[neighborsNum];

      neighborsNum = 0;
      for(int i = 0; i < edgeTypes.length; i++) {
        int edgeType = edgeTypes[i];
        if(edgeType >= 0 && edgeType < node.getEdgeTypes().length) {
          // First get store position for this edge type
          int startIndex = edgeType > 0 ? node.getNeigborGroupIndices()[i - 1] : 0;
          int endIndex = node.getNeigborGroupIndices()[edgeType];
          int len = endIndex - startIndex;

          // Just copy the node ids to the result array
          System.arraycopy(node.getNeighbors(), startIndex, neighborNodeIds, neighborsNum, len);

          // Get neighbor node weight
          for(int j = 0; j < len; j++) {
            float preSumWeight = (startIndex + j) == 0 ? 0 : node.getNeighborAccSumWeights()[startIndex + j - 1];
            neighborWeights[neighborsNum + j] = node.getNeighborAccSumWeights()[startIndex + j] - preSumWeight;

            validEdgeTypes[neighborsNum + j] = edgeType;
          }

          neighborsNum += len;
        }
      }

      return new NodeIDWeightPairs(validEdgeTypes, neighborNodeIds, neighborWeights);
    } else {
      return null;
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int [] offsets = ((GetFullNeighborParam) param).getOffsets();
    long [] nodeIds = ((GetFullNeighborParam) param).getNodeIds();
    int len = ((GetFullNeighborParam) param).getNodeIds().length;

    Map<Long, NodeIDWeightPairs> neighbors = new HashMap(len);
    for(PartitionGetResult result : partResults) {
      PartGetFullNeighborResult getNeighborResult = (PartGetFullNeighborResult) result;
      NodeIDWeightPairs[] nodeResults = getNeighborResult.getNeighborIndices();
      int startIndex = getNeighborResult.getPartId() == 0 ? 0 : offsets[getNeighborResult.getPartId() - 1];
      int endIndex = offsets[getNeighborResult.getPartId()];
      for(int i = startIndex; i < endIndex; i++) {
        neighbors.put(nodeIds[i], nodeResults[i - startIndex]);
      }
    }

    return new GetFullNeighborResult(neighbors);
  }
}
