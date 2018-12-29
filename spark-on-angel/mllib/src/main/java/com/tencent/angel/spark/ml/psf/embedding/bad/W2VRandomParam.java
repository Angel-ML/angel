package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class W2VRandomParam extends UpdateParam {

  int dimension;

  public W2VRandomParam(int matrixId, int dimension) {
    super(matrixId);
    this.dimension = dimension;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionUpdateParam> params = new ArrayList<>();
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    for (PartitionKey pkey: pkeys) {
      params.add(new W2VRandomPartitionParam(matrixId,
        pkey,
        dimension));
    }
    return params;
  }
}
