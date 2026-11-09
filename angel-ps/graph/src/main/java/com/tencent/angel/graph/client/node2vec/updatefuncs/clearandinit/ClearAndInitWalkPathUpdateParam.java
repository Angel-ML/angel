package com.tencent.angel.graph.client.node2vec.updatefuncs.clearandinit;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class ClearAndInitWalkPathUpdateParam extends UpdateParam {

  public ClearAndInitWalkPathUpdateParam(int matrixId, boolean updateClock) {
    super(matrixId, updateClock);
  }

  public ClearAndInitWalkPathUpdateParam(int matrixId) {
    super(matrixId);
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionUpdateParam> partParams = new ArrayList<>(size);
    for (PartitionKey part : parts) {
      partParams.add(new ClearAndInitWalkPathUpdatePartitionParam(matrixId, part));
    }

    return partParams;
  }
}
