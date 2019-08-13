package com.tencent.angel.spark.ml.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetHyperLogLogResult extends GetResult {
  private Long2ObjectOpenHashMap<HyperLogLogPlus> results;

  public GetHyperLogLogResult(Long2ObjectOpenHashMap<HyperLogLogPlus> logs) {
    this.results = logs;
  }

  public Long2ObjectOpenHashMap<HyperLogLogPlus> getResults() {
    return results;
  }
}
