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

package com.tencent.angel.graph.client;

import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighbor;
import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighborParam;
import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighborResult;
import com.tencent.angel.graph.client.sampleneighbor.SampleNeighbor;
import com.tencent.angel.graph.client.sampleneighbor.SampleNeighborParam;
import com.tencent.angel.graph.client.sampleneighbor.SampleNeighborResult;
import com.tencent.angel.graph.data.EdgeId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import java.util.Map;

/**
 * PS Graph
 */
public class PSGraph implements IGraph {

  /**
   * PS matrix client
   */
  private final MatrixClient matrixClient;

  public PSGraph(MatrixClient matrixClient) {
    this.matrixClient = matrixClient;
  }

  @Override
  public Map<Long, NodeIDWeightPairs> biasedSampleNeighbor(long[] nodeIds, long[] parentNodeIds,
      int[] edgeTypes, int[] parentEdgeTypes, int count, float p, float q) {
    return null;
  }

  @Override
  public long[] sampleNode(int nodeType, int count) {
    return new long[0];
  }

  @Override
  public EdgeId[] sampleEdge(int edgeType, int count) {
    return new EdgeId[0];
  }

  @Override
  public int[][] getNodeType(long[] nodeIds) {
    return new int[0][];
  }

  @Override
  public float[][] getNodeFloatFeature(long[] nodeIds, int[] fids) {
    return new float[0][];
  }

  @Override
  public long[][] getNodeLongFeature(long[] nodeIds, int[] fids) {
    return new long[0][];
  }

  @Override
  public String[][] getNodeBinaryFeature(long[] nodeIds, int[] fids) {
    return new String[0][];
  }

  @Override
  public float[][] getEdgeFloatFeature(EdgeId[] edgeIds, int[] fids) {
    return new float[0][];
  }

  @Override
  public long[][] getEdgeLongFeature(EdgeId[] edgeIds, int[] fids) {
    return new long[0][];
  }

  @Override
  public String[][] getEdgeBinaryFeature(EdgeId[] edgeIds, int[] fids) {
    return new String[0][];
  }

  @Override
  public Map<Long, NodeIDWeightPairs> getFullNeighbor(long[] nodeIds, int[] edgeTypes) {
    return ((GetFullNeighborResult) matrixClient.get(new GetFullNeighbor(
        new GetFullNeighborParam(matrixClient.getMatrixId(), nodeIds, edgeTypes))))
        .getNodeIdToNeighbors();
  }

  @Override
  public Map<Long, NodeIDWeightPairs> getSortedFullNeighbor(long[] nodeIds, int[] edgeTypes) {
    return null;
  }

  @Override
  public Map<Long, NodeIDWeightPairs> getTopKNeighbor(long[] nodeIds, int[] edgeTypes, int k) {
    return null;
  }

  @Override
  public Map<Long, NodeIDWeightPairs> sampleNeighbor(long[] nodeIds, int[] edgeTypes, int count) {
    return ((SampleNeighborResult) matrixClient.get(new SampleNeighbor(
            new SampleNeighborParam(matrixClient.getMatrixId(), nodeIds, edgeTypes, count))))
            .getNodeIdToNeighbors();
  }
}
