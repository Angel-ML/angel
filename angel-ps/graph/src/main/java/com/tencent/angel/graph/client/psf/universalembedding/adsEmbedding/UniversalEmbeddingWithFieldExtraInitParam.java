package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.psf.universalembedding.UniversalEmbeddingExtraInitParam;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class UniversalEmbeddingWithFieldExtraInitParam extends UniversalEmbeddingExtraInitParam {

  private int fieldId;

  public UniversalEmbeddingWithFieldExtraInitParam(int matrixId, long[] nodeIds,
      IElement[] embeddings, int fieldId,
      int dim, int numSlots) {
    super(matrixId, nodeIds, embeddings, dim, numSlots);
    this.fieldId = fieldId;
  }

  public UniversalEmbeddingWithFieldExtraInitParam(int matrixId, long[] nodeIds,
      IElement[] embeddings, int dim, int numSlots) {
    this(-1, null, null, -1, -1, -1);
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] splits = RouterUtils.split(meta, 0, getNodeIds(), getEmbeddings());
    assert parts.length == splits.length;

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartUniversalEmbeddingWithFieldExtraInitParam(matrixId, parts[i], splits[i],
                fieldId,
                getDim(), getNumSlots()));
      }
    }

    return partParams;
  }
}
