package com.tencent.angel.graph.client.getnodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetNodeFeatsParam extends GetParam {

  /**
   * Node ids
   */
  private final int[] nodeIds;

  private final List<PartitionGetParam> partParams;

  public GetNodeFeatsParam(int matrixId, int[] nodeIds) {
    super(matrixId);
    this.nodeIds = nodeIds;
    partParams = new ArrayList<>();
  }

  public GetNodeFeatsParam() {
    this(-1, null);
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public List<PartitionGetParam> getPartParams() {
    return partParams;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(nodeIds);

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
        partParams.add(new PartGetNodeFeatsParam(matrixId,
            partitions.get(partIndex), nodeIds, nodeIndex - length, nodeIndex));
      }
      partIndex++;
    }

    return partParams;
  }
}
