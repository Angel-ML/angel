package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class MyPullResult extends GetResult {
  private long[] keys;
  private float[] vals;
  public MyPullResult(long[] keys, float[] vals) {
    this.keys = keys;
    this.vals = vals;
  }

  public long[] getKeys() {
    return keys;
  }

  public float[] getValues() {
    return vals;
  }
}
