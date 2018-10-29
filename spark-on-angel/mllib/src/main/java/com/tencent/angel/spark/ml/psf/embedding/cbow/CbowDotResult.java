package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class CbowDotResult extends GetResult {
  private float[] values;

  public CbowDotResult(float[] values) {
    this.values = values;
  }

  public float[] getValues() {
    return values;
  }
}
