package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class SampleNeighborByNameParam extends SampleNeighborParam {

  /**
   * sample neighbors by node name
   */
  protected final String name;

  public SampleNeighborByNameParam(int matrixId, long[] nodeIds, int count, String name) {
    super(matrixId, nodeIds, count);
    this.name = name;
  }

  public SampleNeighborByNameParam() {
    this(-1, null, -1, "");
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyPart[] splits = RouterUtils.split(meta, 0, nodeIds);
    assert partitions.length == splits.length;

    // Generate node ids
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartSampleNeighborByNameParam(matrixId, partitions[i], splits[i], count, name));
      }
    }

    return partParams;
  }

  public String getName() {
    return name;
  }
}