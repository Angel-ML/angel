package com.tencent.angel.graph.client.sampleFeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class SampleNodeFeatsParam extends GetParam {
  private final int size;

  public SampleNodeFeatsParam(int matrixId, int size) {
    super(matrixId);
    this.size = size;
  }

  public SampleNodeFeatsParam() {
    this(-1, 0);
  }

  @Override
  public List<PartitionGetParam> split() {

    List<PartitionGetParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int eachSize = size / parts.size() + 1;
    for (PartitionKey key : parts)
      params.add(new SampleNodeFeatsPartParam(matrixId, key, eachSize));

    return params;
  }
}
