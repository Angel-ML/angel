package com.tencent.angel.graph.client.node2vec.getfuncs.isWalkFinished;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class IsWalkFinishedParam extends GetParam {

  public IsWalkFinishedParam(int matrixId) {
    super(matrixId);
  }

  public IsWalkFinishedParam() {
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<>(size);
    for (PartitionKey part : parts) {
      partParams.add(new IsWalkFinishedPartitionParam(matrixId, part));
    }

    return partParams;
  }
}
