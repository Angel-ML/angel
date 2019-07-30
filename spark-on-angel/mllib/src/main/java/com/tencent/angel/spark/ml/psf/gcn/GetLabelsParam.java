package com.tencent.angel.spark.ml.psf.gcn;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetLabelsParam extends GetParam {
  private long[] nodes;

  public GetLabelsParam(int matrixId, long[] nodes) {
    super(matrixId);
    this.nodes = nodes;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(nodes);

    List<PartitionGetParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(nodes, parts)) {
      throw new AngelException(
        "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
          .get(parts.size() - 1).getEndCol());
    }

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < nodes.length || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (nodeIndex < nodes.length && nodes[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0)
        params.add(new GetLabelsPartParam(matrixId, parts.get(partIndex),
          nodes, nodeIndex - length, nodeIndex));

      partIndex++;
    }

    return params;
  }
}
