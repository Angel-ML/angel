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

public class SampleNeighborWithFilterParam extends SampleNeighborWithTypeParam {

  protected final long[] filterWithNeighKeys;
  protected final long[] filterWithoutNeighKeys;

  public SampleNeighborWithFilterParam(int matrixId, long[] nodeIds, int count,
      SampleType sampleType, long[] filterWithoutNeighKeys, long[] filterWithNeighKeys,
      NeighborStorageType storageType) {
    super(matrixId, nodeIds, count, sampleType, SampleMethod.RANDOM, storageType);
    this.filterWithNeighKeys = filterWithNeighKeys;
    this.filterWithoutNeighKeys = filterWithoutNeighKeys;
  }

  public SampleNeighborWithFilterParam() {
    this(-1, null, -1, null, null, null, NeighborStorageType.LONGARRAY);
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, filterWithoutNeighKeys);
    assert partitions.length == splits.length;

    // Generate node ids
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartSampleNeighborWithFilterParam(matrixId, partitions[i], count, sampleType,
                splits[i], filterWithNeighKeys, storageType));
      }
    }

    return partParams;
  }

  public long[] getFilterWithNeighKeys() {
    return filterWithNeighKeys;
  }

  public long[] getFilterWithoutNeighKeys() {
    return filterWithoutNeighKeys;
  }
}
