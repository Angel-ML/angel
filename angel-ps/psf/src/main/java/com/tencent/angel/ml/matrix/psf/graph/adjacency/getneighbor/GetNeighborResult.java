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

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import java.util.Map;

/**
 * Result of GetNeighbor
 */
public class GetNeighborResult extends GetResult {

  /**
   * Node id to neighbors map
   */
  private Map<Integer, int []> nodeIdToNeighborIndices;

  GetNeighborResult(Map<Integer, int []> nodeIdToNeighborIndices) {
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
  }

  public Map<Integer, int[]> getNodeIdToNeighborIndices() {
    return nodeIdToNeighborIndices;
  }

  public void setNodeIdToNeighborIndices(Map<Integer, int[]> nodeIdToNeighborIndices) {
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
  }
}
