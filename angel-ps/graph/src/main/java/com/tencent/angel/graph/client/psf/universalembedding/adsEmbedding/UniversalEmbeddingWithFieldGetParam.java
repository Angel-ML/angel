package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class UniversalEmbeddingWithFieldGetParam extends GetParam {

  /**
   * Node ids
   */
  private final long[] nodeIds;

  /**
   * field ids
   */
  private int[] fieldIds;

  public UniversalEmbeddingWithFieldGetParam(int matrixId, long[] nodeIds, int[] fieldIds) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.fieldIds = fieldIds;
  }

  public UniversalEmbeddingWithFieldGetParam() {
    this(-1, null, null);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getFieldIds() {
    return fieldIds;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    // Split
    KeyValuePart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, fieldIds, false);

    // Generate Part psf get param
    List<PartitionGetParam> partParams = new ArrayList<>(parts.length);
    assert parts.length == nodeIdsParts.length;
    for (int i = 0; i < parts.length; i++) {
      if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
        partParams.add(
            new PartUniversalEmbeddingWithFieldGetParam(matrixId, parts[i], nodeIdsParts[i]));
      }
    }

    return partParams;
  }
}
