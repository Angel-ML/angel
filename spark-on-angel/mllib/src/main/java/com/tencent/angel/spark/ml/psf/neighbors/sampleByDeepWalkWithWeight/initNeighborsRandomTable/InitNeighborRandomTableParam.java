package com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.initNeighborsRandomTable;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.sampleRandomNeighborTable.NeighborsRandomTableElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ps part data into partitions ,and each partition handled by PartInitNeighborAttrTagParam
 */
public class InitNeighborRandomTableParam extends UpdateParam {

  private Long2ObjectMap<NeighborsRandomTableElement> nodeId2Neighbors;

  public InitNeighborRandomTableParam(int matrixId,
      Long2ObjectMap<NeighborsRandomTableElement> neighbors) {
    super(matrixId);
    this.nodeId2Neighbors = neighbors;
  }

  public InitNeighborRandomTableParam(int matrixId) {
    super(matrixId);
  }

  @Override
  public List<PartitionUpdateParam> split() {
    long[] nodeIds = nodeId2Neighbors.keySet().toLongArray();
    Arrays.sort(nodeIds);

    List<PartitionUpdateParam> partParams = new ArrayList<>();
    List<PartitionKey> partitions = PSAgentContext.get().getMatrixMetaManager()
        .getPartitions(matrixId);

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
        partParams.add(new PartInitNeighborRandomTableParam(matrixId,
            partitions.get(partIndex), nodeId2Neighbors, nodeIds, nodeIndex - length,
            nodeIndex));
      }
      partIndex++;
    }

    return partParams;
  }

}
