package com.tencent.angel.spark.ml.psf.neighbors;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNeighborResult extends GetResult {

  /**
   * Node id to neighbors map
   */
  private Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors;

  SampleNeighborResult(Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors) {
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }

  public Long2ObjectOpenHashMap<long[]> getNodeIdToNeighbors() {
    return nodeIdToNeighbors;
  }

  public void setNodeIdToNeighbors(
      Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors) {
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }
}
