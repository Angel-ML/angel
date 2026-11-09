package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysIntValuesPart;

import java.util.ArrayList;
import java.util.List;

public class SampleWithTagParam extends GetParam {
  private final long[] nodeIds;
  private final int[] tag;
  private final List<PartitionGetParam> partParams;

  public SampleWithTagParam(int matrixId, long[] nodeIds, int[] tag) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.tag = tag;
    this.partParams = new ArrayList<>();
  }

  public SampleWithTagParam() {
    this(-1, null, null);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getTag() {
    return tag;
  }

  public List<PartitionGetParam> getPartParams() {
    return partParams;
  }

  @Override
  public List<PartitionGetParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, tag, false);
    assert parts.length == nodeIdsParts.length;
    for (int i = 0; i < parts.length; i++) {
      if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
        HashLongKeysIntValuesPart nodeIdsPart = (HashLongKeysIntValuesPart) nodeIdsParts[i];
        partParams.add(new PartSampleWithTagParam(matrixId, parts[i], nodeIdsPart.getKeys(), nodeIdsPart.getValues()));
      }
    }

    return partParams;
  }
}