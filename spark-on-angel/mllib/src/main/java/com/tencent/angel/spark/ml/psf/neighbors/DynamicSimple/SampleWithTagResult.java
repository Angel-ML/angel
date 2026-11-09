package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class SampleWithTagResult extends GetResult {
  private final long[] nodeIds;
  private final int[] tags;

  public SampleWithTagResult(long[] nodeIds, int[] tags) {
    this.nodeIds = nodeIds;
    this.tags = tags;
  }

  public long[] getNodeIds() { return nodeIds; }

  public int[] getTags() { return tags; }
}