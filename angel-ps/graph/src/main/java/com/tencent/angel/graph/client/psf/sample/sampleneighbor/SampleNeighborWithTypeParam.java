package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class SampleNeighborWithTypeParam extends SampleNeighborParam {

  /**
   * sample types, such as: simple, withNodeType, withEdgeType
   */
  protected final SampleType sampleType;

  public SampleNeighborWithTypeParam(int matrixId, long[] nodeIds, int count, SampleType sampleType,
      NeighborStorageType storageType) {
    super(matrixId, nodeIds, count, storageType);
    this.sampleType = sampleType;
  }

  public SampleNeighborWithTypeParam(int matrixId, long[] nodeIds, int count, SampleType sampleType,
      SampleMethod sampleMethod, NeighborStorageType storageType) {
    super(matrixId, nodeIds, count, sampleMethod, storageType);
    this.sampleType = sampleType;
  }

  public SampleNeighborWithTypeParam() {
    this(-1, null, -1, null, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
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
            new PartSampleNeighborWithTypeParam(matrixId, partitions[i], splits[i], count,
                sampleType, method, storageType));
      }
    }

    return partParams;
  }

  public SampleType getSampleType() {
    return sampleType;
  }
}
