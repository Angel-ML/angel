package com.tencent.angel.graph.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class UpdateReadCounterParam extends UpdateParam {

  public UpdateReadCounterParam(int matrixId) {
    super(matrixId);
  }

  @Override
  public List<PartitionUpdateParam> split() {

    List<PartitionUpdateParam> params = new ArrayList<>();
    //List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    for (PartitionKey key : parts)
      params.add(new UpdateReadCounterPartParam(matrixId, key));

    return params;
  }
}