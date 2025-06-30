package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNeighborResultByType extends SampleNeighborResult {

  SampleNeighborResultByType(Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors) {
    super(nodeIdToSampleNeighbors);
  }

  SampleNeighborResultByType() {
    this(null);
  }

}