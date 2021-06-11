package com.tencent.angel.graph.model.neighbor.simplewithtype.psf.sample;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class SampleParam extends LongKeysGetParam {
  private final int sampleType;

  public SampleParam(int matrixId, long[] nodeIds, int sampleType) {
    super(matrixId, nodeIds);
    this.sampleType = sampleType;
  }

  public SampleParam() {
    this(-1, null, -1);
  }

  public int getSampleType() {
    return sampleType;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    // Split
    KeyPart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, false);

    // Generate Part psf get param
    List<PartitionGetParam> partParams = new ArrayList<>(parts.length);
    assert parts.length == nodeIdsParts.length;
    for (int i = 0; i < parts.length; i++) {
      if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
        partParams.add(new PartSampleParam(matrixId, parts[i], nodeIdsParts[i], sampleType));
      }
    }

    return partParams;
  }
}