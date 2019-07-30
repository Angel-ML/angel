package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

public class GetClosenessResult extends GetResult {
  private Long2DoubleOpenHashMap results;

  public GetClosenessResult(Long2DoubleOpenHashMap closenesses) {
    this.results = closenesses;
  }

  public Long2DoubleOpenHashMap getResults() {
    return results;
  }
}
