package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class SampleNeighborWithTimeParam extends SampleNeighborParam {

  private int[] timestamps;

  public SampleNeighborWithTimeParam(int matrixId, long[] nodeIds, int[] timestamps, int count,
      SampleMethod sampleMethod, NeighborStorageType storageType) {
    super(matrixId, nodeIds, count, sampleMethod, storageType);
    this.timestamps = timestamps;
  }

  public SampleNeighborWithTimeParam() {
    this(-1, null, null, -1, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, timestamps, false);
    assert partitions.length == splits.length;

    // Generate node ids
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartSampleNeighborWithTimeParam(matrixId, partitions[i], splits[i], count, method,
                storageType));
      }
    }

    return partParams;
  }
}
