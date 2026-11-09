package com.tencent.angel.graph.client.node2vec.updatefuncs.clearandinit;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;

public class ClearAndInitWalkPathUpdatePartitionParam extends PartitionUpdateParam {

  public ClearAndInitWalkPathUpdatePartitionParam(int matrixId, PartitionKey partKey,
      boolean updateClock) {
    super(matrixId, partKey, updateClock);
  }

  public ClearAndInitWalkPathUpdatePartitionParam() {
    super();
  }

  public ClearAndInitWalkPathUpdatePartitionParam(int matrixId, PartitionKey partKey) {
    super(matrixId, partKey);
  }
}
