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

import com.tencent.angel.graph.data.EdgeId;
import java.util.Map;

/**
 * Graph operator interface
 */
public interface IGraph {

  Map<Long, NodeIDWeightPairs> biasedSampleNeighbor(long[] nodeIds, long[] parentNodeIds, int[] edgeTypes,
      int[] parentEdgeTypes, int count, float p, float q);

  long[] sampleNode(int nodeType, int count);

  EdgeId[] sampleEdge(int edgeType, int count);

  int[] getNodeType(long[] nodeIds);

  float[][] getNodeFloatFeature(long[] nodeIds, int[] fids);

  long[][] getNodeLongFeature(long[] nodeIds, int[] fids);

  String[][] getNodeBinaryFeature(long[] nodeIds, int[] fids);

  float[][] getEdgeFloatFeature(EdgeId[] edgeIds, int[] fids);

  long[][] getEdgeLongFeature(EdgeId[] edgeIds, int[] fids);

  String[][] getEdgeBinaryFeature(EdgeId[] edgeIds, int[] fids);

  /**
   * Get full neighbors for given edge types
   * @param nodeIds node ids
   * @param edgeTypes edge types
   * @return node id to result map
   */
  Map<Long, NodeIDWeightPairs> getFullNeighbor(long[] nodeIds, int[] edgeTypes);

  Map<Long, NodeIDWeightPairs> getSortedFullNeighbor(long[] nodeIds, int[] edgeTypes);

  Map<Long, NodeIDWeightPairs> getTopKNeighbor(long[] nodeIds, int[] edgeTypes, int k);

  Map<Long, NodeIDWeightPairs> sampleNeighbor(long[] nodeIds, int[] edgeTypes, int count);
}
