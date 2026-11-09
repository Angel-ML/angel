package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.graph.client.psf.universalembedding.UniversalEmbeddingInitAsNodes;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Random;

public class UniversalEmbeddingWithFieldInitAsNodes extends UniversalEmbeddingInitAsNodes {

  public UniversalEmbeddingWithFieldInitAsNodes(UniversalEmbeddingWithFieldInitParam param) {
    super(param);
  }

  public UniversalEmbeddingWithFieldInitAsNodes() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartUniversalEmbeddingWithFieldInitParam param = (PartUniversalEmbeddingWithFieldInitParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    ILongKeyPartOp split = (ILongKeyPartOp) param.getIndicesPart();
    int dim = param.getDim();
    int seed = param.getSeed();
    Random rand = new Random(seed);
    int numSlots = param.getNumSlots();
    float[] floats = param.getFloats();
    int[] ints = param.getInts();
    int iniFuncType = ints[ints.length - 1];
    long[] nodeIds = split.getKeys();
    int fieldId = param.getFieldId();

    row.startWrite();
    try {
      for (long nodeId : nodeIds) {
        UniversalEmbeddingNodeWithField embeddingNode = (UniversalEmbeddingNodeWithField) row
            .get(nodeId);
        if (embeddingNode == null) {
          embeddingNode = new UniversalEmbeddingNodeWithField();
        }

        UniversalEmbeddingNode embedding;
        float[] embeddings = new float[dim];
        // 0-RandomUniform, 1-RandomNormal, 2-XavierOrKaimingUniform, 3-XavierOrKaimingNormal
        if (iniFuncType == 0) {
          randomUniform(rand, embeddings, floats, ints);
        } else if (iniFuncType == 1) {
          randomNormal(rand, embeddings, floats, ints);
        } else if (iniFuncType == 2) {
          xavierOrKaimingUniform(rand, embeddings, floats, ints);
        } else if (iniFuncType == 3) {
          xavierOrKaimingNormal(rand, embeddings, floats, ints);
        }

        if (numSlots <= 1) {
          embedding = new UniversalEmbeddingNode(embeddings);
        } else {
          float[] slotsValues = new float[dim * (numSlots - 1)];
          embedding = new UniversalEmbeddingNode(embeddings, slotsValues, numSlots);
        }
        Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = embeddingNode.getFieldId2Emb();
        fieldId2Emb.put(fieldId, embedding);

        Int2LongOpenHashMap fieldId2UpdateTimestamp = embeddingNode.getFieldId2UpdateTimestampSecond();
        fieldId2UpdateTimestamp.put(fieldId, System.currentTimeMillis() / 1000);
        row.set(nodeId, embeddingNode);
      }
    } finally {
      row.endWrite();
    }

  }
}
