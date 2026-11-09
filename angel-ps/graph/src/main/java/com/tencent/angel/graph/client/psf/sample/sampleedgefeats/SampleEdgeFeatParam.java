package com.tencent.angel.graph.client.psf.sample.sampleedgefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleMethod;
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleNeighborParam;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class SampleEdgeFeatParam extends SampleNeighborParam {

  public SampleEdgeFeatParam(int matrixId, long[] nodeIds, int count,
      NeighborStorageType storageType) {
    super(matrixId, nodeIds, count, storageType);
  }

  public SampleEdgeFeatParam(int matrixId, long[] nodeIds, int count, SampleMethod method,
      NeighborStorageType storageType) {
    super(matrixId, nodeIds, count, method, storageType);
  }

  public SampleEdgeFeatParam() {
    this(-1, null, -1, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyPart[] splits = RouterUtils.split(meta, 0, nodeIds);
    assert partitions.length == splits.length;

    // Generate rpc params
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartSampleEdgeFeatParam(matrixId, partitions[i], splits[i], count, method,
                storageType));
      }
    }

    return partParams;
  }
}
