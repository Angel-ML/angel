package com.tencent.angel.spark.ml.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UpdateHyperLogLogParam extends UpdateParam {

  private Long2ObjectOpenHashMap<HyperLogLogPlus> updates;
  private int p;
  private int sp;

  public UpdateHyperLogLogParam(int matrixId, Long2ObjectOpenHashMap<HyperLogLogPlus> updates, int p, int sp) {
    super(matrixId);
    this.updates = updates;
    this.p = p;
    this.sp = sp;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    long[] nodes = updates.keySet().toLongArray();
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
        params.add(new UpdateHyperLogLogPartParam(matrixId,
          parts.get(partIndex), updates, p, sp, nodes, nodeIndex - length,
          nodeIndex));
      }
      partIndex++;
    }
    return params;
  }
}
