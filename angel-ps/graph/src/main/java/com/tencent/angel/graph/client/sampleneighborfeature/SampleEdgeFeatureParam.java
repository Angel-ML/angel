package com.tencent.angel.graph.client.sampleneighborfeature;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleEdgeFeatureParam extends GetParam {

  private final long[] keys;
  private final int numSample;
  private final List<PartitionGetParam> params;
  private final boolean sampleTypes;


  public SampleEdgeFeatureParam(int matrixId, long[] keys, int numSample, boolean sampleTypes) {
    super(matrixId);
    this.keys = keys;
    this.numSample = numSample;
    this.params = new ArrayList<>();
    this.sampleTypes = sampleTypes;
  }

  public SampleEdgeFeatureParam() {
    this(-1, null, -1, false);
  }

  public List<PartitionGetParam> getParams() {
    return params;
  }

  public long[] getKeys() {
    return keys;
  }

  public boolean getSampleTypes() {
    return sampleTypes;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(keys);
    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(keys, partitions)) {
      throw new AngelException(
          "node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
              .get(partitions.size() - 1).getEndCol());
    }

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < keys.length || partIndex < partitions.size()) {
      int length = 0;
      long endOffset = partitions.get(partIndex).getEndCol();
      while (nodeIndex < keys.length && keys[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        params.add(new SampleEdgeFeaturePartParam(matrixId,
            partitions.get(partIndex), numSample, keys, sampleTypes,
            nodeIndex - length, nodeIndex));
      }

      partIndex++;
    }

    return params;
  }

}
