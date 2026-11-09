package com.tencent.angel.graph.client.node2vec.getfuncs.getstats;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class GetStatsResult extends GetResult {

  private long numNode = 0;
  private long numEdge = 0;
  private Int2IntOpenHashMap buckets;

  public GetStatsResult(long numNode, long numEdge, Int2IntOpenHashMap buckets) {
    this.numNode = numNode;
    this.numEdge = numEdge;
    this.buckets = buckets;
  }

  public GetStatsResult() {
  }

  public long getNumNode() {
    return numNode;
  }

  public void setNumNode(long numNode) {
    this.numNode = numNode;
  }

  public long getNumEdge() {
    return numEdge;
  }

  public void setNumEdge(long numEdge) {
    this.numEdge = numEdge;
  }

  public Int2IntOpenHashMap getBuckets() {
    return buckets;
  }

  public void setBuckets(Int2IntOpenHashMap buckets) {
    this.buckets = buckets;
  }
}
