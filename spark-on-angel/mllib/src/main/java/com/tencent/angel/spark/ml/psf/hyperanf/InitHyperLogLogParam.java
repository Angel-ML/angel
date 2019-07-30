package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InitHyperLogLogParam extends UpdateParam {

  private long[] nodes;
  private int p;
  private int sp;

  public InitHyperLogLogParam(int matrixId, int p, int sp, long[] nodes) {
    super(matrixId);
    this.p = p;
    this.sp = sp;
    this.nodes = nodes;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    Arrays.sort(nodes);

    List<PartitionUpdateParam> params = new ArrayList<>();
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

      if (length > 0) {
        params.add(new InitHyperLogLogPartParam(matrixId,
          parts.get(partIndex), nodes, nodeIndex - length, nodeIndex,
          p, sp));
      }
      partIndex++;
    }

    return params;
  }

}
