package com.tencent.angel.graph.kclique;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNodeInfoResult extends GetResult {

  /**
   * Node id to neighbors map
   */
  private Long2ObjectOpenHashMap<long[]> nodeIdToInfos;

  SampleNodeInfoResult(Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors) {
    this.nodeIdToInfos = nodeIdToNeighbors;
  }

  public Long2ObjectOpenHashMap<long[]> getNodeIdToNeighbors() {
    return nodeIdToInfos;
  }

  public void setNodeIdToNeighbors(
          Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors) {
    this.nodeIdToInfos = nodeIdToNeighbors;
  }
}
