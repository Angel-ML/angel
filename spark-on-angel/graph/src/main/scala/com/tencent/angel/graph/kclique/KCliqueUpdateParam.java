package com.tencent.angel.graph.kclique;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KCliqueUpdateParam extends UpdateParam {

  /**
   * Node id to neighbors map
   */
  private final Long2ObjectMap<long[]> nodeIdToNeighborIndices;
  private final int writeRowId;

  public KCliqueUpdateParam(int matrixId, int rowId, Long2ObjectMap<long[]> nodeIdToNeighborIndices) {
    super(matrixId);
    this.writeRowId = rowId;
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    long[] nodeIds = nodeIdToNeighborIndices.keySet().toLongArray();

    Arrays.sort(nodeIds);

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
      long endOffset = partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new KCliquePartUpdateParam(matrixId,
                partitions.get(partIndex), nodeIdToNeighborIndices, nodeIds, nodeIndex - length,
                nodeIndex, writeRowId));
      }
      partIndex++;
    }

    return partParams;
  }
}
