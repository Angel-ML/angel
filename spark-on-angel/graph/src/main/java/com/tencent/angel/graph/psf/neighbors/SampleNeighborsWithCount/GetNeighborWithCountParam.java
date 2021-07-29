package com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class GetNeighborWithCountParam extends GetParam {

  /**
   * Node ids
   */
  private final long[] nodeIds;
  private final int[] count;

  public GetNeighborWithCountParam(int matrixId, long[] nodeIds, int[] count) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.count = count;
  }

  public GetNeighborWithCountParam() {
    this(-1, null, null);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getCount() {
    return count;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    // Split
    KeyValuePart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, count, false);

    // Generate Part psf get param
    List<PartitionGetParam> partParams = new ArrayList<>(parts.length);
    assert parts.length == nodeIdsParts.length;
    for (int i = 0; i < parts.length; i++) {
      if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
        partParams.add(new PartGetNeighborWithCountParam(matrixId, parts[i], nodeIdsParts[i]));
      }
    }

    return partParams;
  }
}