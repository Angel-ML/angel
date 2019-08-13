package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class ComputeClosenessParam extends UpdateParam {

  private int r;

  public ComputeClosenessParam(int matrixId, int r) {
    super(matrixId);
    this.r = r;
  }

  @Override
  public List<PartitionUpdateParam> split() {

    List<PartitionUpdateParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    for (PartitionKey key : parts)
      params.add(new ComputeClosenessPartParam(matrixId, key, r));

    return params;
  }
}
