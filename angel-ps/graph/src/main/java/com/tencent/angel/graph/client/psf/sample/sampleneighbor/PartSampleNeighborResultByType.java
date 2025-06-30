package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartSampleNeighborResultByType extends PartSampleNeighborResult {

  public PartSampleNeighborResultByType(int partId,
      Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors) {
    super(partId, nodeIdToSampleNeighbors);
  }

  public PartSampleNeighborResultByType() {
    this(-1, null);
  }

  public int getPartId() {
    return partId;
  }
}
