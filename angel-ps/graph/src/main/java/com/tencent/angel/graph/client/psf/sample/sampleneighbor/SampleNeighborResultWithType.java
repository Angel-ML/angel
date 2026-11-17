package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.data.Neighbor;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNeighborResultWithType extends SampleNeighborResult {

  /**
   * Node id to neighbors type map
   */
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType;
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType;

  SampleNeighborResultWithType(Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType) {
    super(nodeIdToSampleNeighbors);
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
  }

  SampleNeighborResultWithType(Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors,
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