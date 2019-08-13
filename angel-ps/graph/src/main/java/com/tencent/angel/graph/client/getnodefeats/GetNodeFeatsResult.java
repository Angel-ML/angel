package com.tencent.angel.graph.client.getnodefeats;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class GetNodeFeatsResult extends GetResult {

  private final Int2ObjectOpenHashMap<IntFloatVector> result;

  public GetNodeFeatsResult(Int2ObjectOpenHashMap<IntFloatVector> result) {
    this.result = result;
  }

  public Int2ObjectOpenHashMap<IntFloatVector> getResult() {
    return result;
  }
}
