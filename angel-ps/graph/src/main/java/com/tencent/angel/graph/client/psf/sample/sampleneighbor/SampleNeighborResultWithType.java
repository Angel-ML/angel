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

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNeighborResultWithType extends SampleNeighborResult {

  /**
   * Node id to neighbors type map
   */
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType;
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType;

  SampleNeighborResultWithType(Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType) {
    super(nodeIdToSampleNeighbors);
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
  }

  SampleNeighborResultWithType(Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType) {
    this(nodeIdToSampleNeighbors, nodeIdToSampleNeighborsType, null);
  }

  SampleNeighborResultWithType() {
    this(null, null, null);
  }

  public Long2ObjectOpenHashMap<int[]> getNodeIdToSampleNeighborsType() {
    return nodeIdToSampleNeighborsType;
  }

  public void setNodeIdToSampleNeighborsType(
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType) {
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
  }

  public Long2ObjectOpenHashMap<int[]> getNodeIdToSampleEdgeType() {
    return nodeIdToSampleEdgeType;
  }

  public void setNodeIdToSampleEdgeType(
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType) {
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
  }
}