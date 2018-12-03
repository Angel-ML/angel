package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class W2VPullResult extends GetResult {
  public int[] indices;
  public float[] layers;

  public W2VPullResult(int[] indices, float[] layers) {
    this.indices = indices;
    this.layers = layers;
  }

}
