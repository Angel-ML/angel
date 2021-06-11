package com.tencent.angel.graph.common.psf.result;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetLongsResult extends GetResult {
  private final Long2ObjectOpenHashMap<long[]> data;

  public GetLongsResult(Long2ObjectOpenHashMap<long[]> data) {
    this.data = data;
  }

  public Long2ObjectOpenHashMap<long[]> getData() {
    return data;
  }
}
