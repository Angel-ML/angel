package com.tencent.angel.graph.client.initnodefeats2;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import com.tencent.angel.utils.Sort;
import java.util.ArrayList;
import java.util.List;

public class InitNodeFeatsParam extends UpdateParam {

  private final long[] nodeIds;
  private final IntFloatVector[] feats;

  public InitNodeFeatsParam(int matrixId, long[] nodeIds, IntFloatVector[] feats) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.feats = feats;
    assert nodeIds.length == feats.length;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    Sort.quickSort(nodeIds, feats, 0, nodeIds.length - 1);

    List<PartitionUpdateParam> partParams = new ArrayList<>();
    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(nodeIds, partitions)) {
      throw new AngelException(
          "node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
              .get(partitions.size() - 1).getEndCol());
    }

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < nodeIds.length || partIndex < partitions.size()) {
      int length = 0;
      int endOffset = (int) partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new PartInitNodeFeatsParam(matrixId,
            partitions.get(partIndex), nodeIds, feats, nodeIndex - length, nodeIndex));
      }
      partIndex++;
    }

    return partParams;
  }
}
