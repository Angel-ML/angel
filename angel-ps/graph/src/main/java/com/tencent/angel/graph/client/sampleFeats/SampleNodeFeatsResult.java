package com.tencent.angel.graph.client.sampleFeats;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import java.util.Collections;
import java.util.List;

public class SampleNodeFeatsResult extends GetResult {

  /**
   * randomly sampled node features array
   */

  private final List<IntFloatVector> result;

  public SampleNodeFeatsResult(List<IntFloatVector> result) {
    this.result = result;
  }

  public IntFloatVector[] getResult() {
    Collections.shuffle(result);
    IntFloatVector[] res = new IntFloatVector[result.size()];
    return result.toArray(res);
  }
}
