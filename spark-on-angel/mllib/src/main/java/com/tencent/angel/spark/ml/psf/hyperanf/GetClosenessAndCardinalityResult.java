package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple2;
import scala.Tuple3;

public class GetClosenessAndCardinalityResult extends GetResult {
  private Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> results;

  public GetClosenessAndCardinalityResult(Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses) {
    this.results = closenesses;
  }

  public Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> getResults() {
    return results;
  }
}
