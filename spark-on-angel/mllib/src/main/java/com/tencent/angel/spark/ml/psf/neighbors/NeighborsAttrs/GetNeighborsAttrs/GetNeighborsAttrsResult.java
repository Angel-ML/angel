package com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.GetNeighborsAttrs;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs.NeighborsAttrsElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetNeighborsAttrsResult extends GetResult {

  private Long2ObjectOpenHashMap<NeighborsAttrsElement> nodeIdeToNeighbors;

  public GetNeighborsAttrsResult(Long2ObjectOpenHashMap<NeighborsAttrsElement> nodeIdeToNeighbors) {
    this.nodeIdeToNeighbors = nodeIdeToNeighbors;
  }

  public Long2ObjectOpenHashMap<NeighborsAttrsElement> getNodeIdeToNeighbors() {
    return nodeIdeToNeighbors;
  }

  public void setNodeIdeToNeighbors(
      Long2ObjectOpenHashMap<NeighborsAttrsElement> nodeIdeToNeighbors) {
    this.nodeIdeToNeighbors = nodeIdeToNeighbors;
  }
}
