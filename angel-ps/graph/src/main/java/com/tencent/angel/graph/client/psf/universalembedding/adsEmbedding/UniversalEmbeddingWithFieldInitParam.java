package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.psf.universalembedding.UniversalEmbeddingInitParam;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class UniversalEmbeddingWithFieldInitParam extends UniversalEmbeddingInitParam {

  private int fieldId;

  public UniversalEmbeddingWithFieldInitParam(int matrixId, long[] nodeIds, int fieldId, int seed,
      int dim,
      int numSlots, float[] floats, int[] ints) {
    super(matrixId, nodeIds, seed, dim, numSlots, floats, ints);
    this.fieldId = fieldId;
  }

  public UniversalEmbeddingWithFieldInitParam(int matrixId, long[] nodeIds, int fieldId, int dim) {
    this(matrixId, nodeIds, fieldId, -1, dim, 1, new float[0], new int[]{dim});
  }

  public UniversalEmbeddingWithFieldInitParam() {
    this(-1, null, -1, -1, -1, -1, null, null);
  }


  @Override
  public List<PartitionUpdateParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyPart[] splits = RouterUtils.split(meta, 0, getNodeIds());
    assert partitions.length == splits.length;

    // Generate rpc params
    List<PartitionUpdateParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartUniversalEmbeddingWithFieldInitParam(matrixId, partitions[i], splits[i],
                fieldId,
                getSeed(), getDim(), getNumSlots(), getFloats(), getInts()));
      }
    }

    return partParams;
  }
}
