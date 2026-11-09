package com.tencent.angel.graph.client.psf.sample.sampleedgefeats;

import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleNeighborResult;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.Neighbor;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleEdgeFeatResult extends SampleNeighborResult {

  /**
   * Node id to neighbors edge feature map
   */
  private Long2ObjectOpenHashMap<Feature[]> nodeIdToSampleEdgeFeats;

  SampleEdgeFeatResult(Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<Feature[]> nodeIdToSampleEdgeFeats) {
    super(nodeIdToSampleNeighbors);
    this.nodeIdToSampleEdgeFeats = nodeIdToSampleEdgeFeats;
  }

  SampleEdgeFeatResult() {
    this(null, null);
  }

  public Long2ObjectOpenHashMap<Feature[]> getNodeIdToSampleEdgeFeats() {
    return nodeIdToSampleEdgeFeats;
  }

  public void setNodeIdToSampleEdgeFeats(
      Long2ObjectOpenHashMap<Feature[]> nodeIdToSampleEdgeFeats) {
    this.nodeIdToSampleEdgeFeats = nodeIdToSampleEdgeFeats;
  }
}