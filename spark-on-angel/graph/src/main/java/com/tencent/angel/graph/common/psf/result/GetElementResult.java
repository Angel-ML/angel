package com.tencent.angel.graph.common.psf.result;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetElementResult extends GetResult {
  private final Long2ObjectOpenHashMap data;

  public GetElementResult(Long2ObjectOpenHashMap data) {
    this.data = data;
  }

  public Long2ObjectOpenHashMap getData() {
    return data;
  }
}
