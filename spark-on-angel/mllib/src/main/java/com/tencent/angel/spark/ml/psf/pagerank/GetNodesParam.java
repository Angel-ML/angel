package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetNodesParam extends GetParam {

  private int[] partitionIds;

  public GetNodesParam(int matrixId, int[] partitionIds) {
    super(matrixId);
    this.partitionIds = partitionIds;
  }

  public GetNodesParam() {
    super();
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionGetParam> partParams = new ArrayList<>(partitionIds.length);

    Map<Integer, PartitionKey> partsMap = new HashMap<>();
    for (PartitionKey pkey: parts)
      partsMap.put(pkey.getPartitionId(), pkey);

    for (int i = 0; i < partitionIds.length; i++)
      partParams.add(new PartitionGetParam(matrixId, partsMap.get(partitionIds[i])));
    return partParams;
  }
}
