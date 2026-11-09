package com.tencent.angel.spark.ml.psf.neighbors;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetNeighborAttrTagResult extends GetResult {

  private Long2ObjectOpenHashMap<NeighborsAttrTagElement> nodeIdeToNeighbors;

  public GetNeighborAttrTagResult(
      Long2ObjectOpenHashMap<NeighborsAttrTagElement> nodeIdeToNeighbors) {
    this.nodeIdeToNeighbors = nodeIdeToNeighbors;
  }

  public Long2ObjectOpenHashMap<NeighborsAttrTagElement> getNodeIdeToNeighbors() {
    return nodeIdeToNeighbors;
  }

  public void setNodeIdeToNeighbors(
      Long2ObjectOpenHashMap<NeighborsAttrTagElement> nodeIdeToNeighbors) {
    this.nodeIdeToNeighbors = nodeIdeToNeighbors;
  }
}
