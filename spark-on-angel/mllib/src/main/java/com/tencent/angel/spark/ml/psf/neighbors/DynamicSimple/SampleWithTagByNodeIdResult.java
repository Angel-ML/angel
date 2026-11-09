package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class SampleWithTagByNodeIdResult extends GetResult {
  private final long[] nodeIds;
  private final long[] neighbors;
  private final int[] tags;

  public SampleWithTagByNodeIdResult(long[] nodeIds, long[] neighbors, int[] tags) {
    this.nodeIds = nodeIds;
    this.neighbors = neighbors;
    this.tags = tags;
  }

  public long[] getNodeIds() { return nodeIds; }

  public long[] getNeighbors() {return neighbors; }

  public int[] getTags() { return tags; }
}