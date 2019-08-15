package com.tencent.angel.graph.client.getnodefeats2;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetNodeFeatsResult extends GetResult {

  private final Long2ObjectOpenHashMap<IntFloatVector> result;

  public GetNodeFeatsResult(Long2ObjectOpenHashMap<IntFloatVector> result) {
    this.result = result;
  }

  public Long2ObjectOpenHashMap<IntFloatVector> getResult() {
    return result;
  }
}
