package com.tencent.angel.graph.kclique;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The parameter of SampleNeighbor
 */
public class KCliqueGetParam extends GetParam {

  /**
   * Node ids
   */
  private final long[] nodeIds;

  /**
   * Sample neighbor number, if count <= 0, means get all neighbors
   */
  private final int count;

  private final List<PartitionGetParam> partParams;

  private final int readRowId;

  public KCliqueGetParam(int matrixId, int rowId, long[] nodeIds, int count) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.count = count;
    this.partParams = new ArrayList<>();
    this.readRowId = rowId;
  }

  public KCliqueGetParam() {
    this(-1, 0, null, -1);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int getCount() {
    return count;
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
      long endOffset = partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new KCliquePartGetParam(matrixId,
                partitions.get(partIndex), count, nodeIds, nodeIndex - length, nodeIndex, readRowId));
      }
      partIndex++;
    }

    return partParams;
  }
}
