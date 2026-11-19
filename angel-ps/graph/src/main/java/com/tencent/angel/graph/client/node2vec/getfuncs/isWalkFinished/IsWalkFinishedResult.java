package com.tencent.angel.graph.client.node2vec.getfuncs.isWalkFinished;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class IsWalkFinishedResult extends GetResult {

  private long count;

  public IsWalkFinishedResult(long count) {
    this.count = count;
  }

  public boolean isWalkFinished() {
    return count == 0;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }
}
